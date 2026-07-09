import os

def split_markdown(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    # Find a good split point near the middle
    midpoint = len(lines) // 2
    split_index = midpoint
    
    # Try to find a User message to split on, so we don't break an AI thought process
    for i in range(midpoint, len(lines)):
        if lines[i].startswith("### 🧑 User"):
            split_index = i
            break
            
    part1_lines = lines[:split_index]
    part2_lines = lines[split_index:]
    
    # Add indicators
    part1_lines.append("\n\n---\n> **NOTE:** This conversation log was too large and has been chunked. It continues in **Part 2**.\n")
    
    part2_lines.insert(0, "> **NOTE:** This is **Part 2** of the conversation log. It continues from Part 1.\n---\n\n")
    
    base, ext = os.path.splitext(file_path)
    part1_path = f"{base}_part1{ext}"
    part2_path = f"{base}_part2{ext}"
    
    with open(part1_path, 'w', encoding='utf-8') as f:
        f.writelines(part1_lines)
        
    with open(part2_path, 'w', encoding='utf-8') as f:
        f.writelines(part2_lines)
        
    print(f"Successfully split into:\n- {part1_path}\n- {part2_path}")

if __name__ == "__main__":
    split_markdown("D:/exploratory/duckdb-extension/logs/arrowtiles/conversation_log_2026_06_12.md")
