use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{Connection, Result, core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId}};
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Mutex, Arc};
use std::thread;
use arrow::ipc::writer::FileWriter;
use std::fs::File;

// We will send the query and filepath to a background thread that holds the connection
type Request = (String, String, mpsc::Sender<std::result::Result<usize, String>>);
type SenderMutex = Arc<Mutex<mpsc::Sender<Request>>>;

struct ArrowTilesVTab;

struct ArrowTilesBindData {
    query: String,
    filepath: String,
    tx: SenderMutex,
}

struct ArrowTilesInitData {
    done: AtomicBool,
}

impl VTab for ArrowTilesVTab {
    type InitData = ArrowTilesInitData;
    type BindData = ArrowTilesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("rows_exported", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        
        let query = bind.get_parameter(0).to_string();
        let filepath = bind.get_parameter(1).to_string();
        
        // Get the sender from extra_info
        let tx = unsafe { (*bind.get_extra_info::<SenderMutex>()).clone() };
        
        Ok(ArrowTilesBindData { query, filepath, tx })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ArrowTilesInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            // Send request to background thread
            let (reply_tx, reply_rx) = mpsc::channel();
            bind_data.tx.lock().unwrap().send((bind_data.query.clone(), bind_data.filepath.clone(), reply_tx)).unwrap();
            
            // Wait for background thread to finish
            match reply_rx.recv().unwrap() {
                Ok(rows) => {
                    let mut vector = output.flat_vector(0);
                    unsafe {
                        vector.as_mut_slice::<i64>()[0] = rows as i64;
                    }
                    output.set_len(1);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // query
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // filepath
        ])
    }
}

#[duckdb_loadable_macros::duckdb_entrypoint_c_api(ext_name="arrowtiles")]
pub unsafe fn arrowtiles_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    println!("🚀 ArrowTiles Extension loaded. Initializing background worker...");

    let (tx, rx) = mpsc::channel::<Request>();
    let tx_mutex = Arc::new(Mutex::new(tx));

    // Register our table function first
    conn.register_table_function_with_extra_info::<ArrowTilesVTab, _>("arrowtiles_export", &tx_mutex)?;

    // Spawn the background thread that actually executes the queries
    thread::spawn(move || {
        while let Ok((query, filepath, reply_tx)) = rx.recv() {
            println!("ArrowTiles Worker: Executing inner query...");
            // Execute the inner query
            match conn.prepare(&query) {
                Ok(mut stmt) => {
                    match stmt.query_arrow([]) {
                        Ok(arrow_result) => {
                            let mut rows_exported = 0;
                            let mut writer: Option<FileWriter<File>> = None;
                            let mut success = true;

                            for batch in arrow_result {
                                if writer.is_none() {
                                    match File::create(&filepath) {
                                        Ok(file) => {
                                            match FileWriter::try_new(file, batch.schema().as_ref()) {
                                                Ok(w) => writer = Some(w),
                                                Err(e) => {
                                                    let _ = reply_tx.send(Err(format!("Arrow writer error: {}", e)));
                                                    success = false;
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let _ = reply_tx.send(Err(format!("File creation error: {}", e)));
                                            success = false;
                                            break;
                                        }
                                    }
                                }

                                if let Some(w) = writer.as_mut() {
                                    if let Err(e) = w.write(&batch) {
                                        let _ = reply_tx.send(Err(format!("Arrow write error: {}", e)));
                                        success = false;
                                        break;
                                    }
                                }
                                rows_exported += batch.num_rows();
                            }

                            if success {
                                if let Some(mut w) = writer {
                                    if let Err(e) = w.finish() {
                                        let _ = reply_tx.send(Err(format!("Arrow finish error: {}", e)));
                                        success = false;
                                    }
                                }
                                if success {
                                    let _ = reply_tx.send(Ok(rows_exported));
                                }
                            }
                        }
                        Err(e) => {
                            let _ = reply_tx.send(Err(format!("Query arrow error: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    let _ = reply_tx.send(Err(format!("Prepare error: {}", e)));
                }
            }
        }
    });

    Ok(())
}
