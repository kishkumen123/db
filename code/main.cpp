#define _CRT_SECURE_NO_DEPRECATE 1

#include "base_inc.h"
#include "win32_base_inc.h"

global Arena* pm = alloc_arena(MB(4));
global Arena* tm = alloc_arena(MB(1));
global String8 dir = os_get_cwd(pm);
global String8 filename = str8_literal("\\data\\mydb.db");
global bool running = true;

global u32 const ID_SIZE = sizeof(s32);
global u32 const USERNAME_SIZE = 32;
global u32 const EMAIL_SIZE = 255;
typedef struct Row{
    u32 id;
    char username[USERNAME_SIZE];
    char email[EMAIL_SIZE];
} Row;

global u32 ID_OFFSET = 0;
global u32 USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
global u32 EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
global u32 ROW_SIZE = sizeof(Row);
global u32 PAGE_SIZE = KB(4);
global u32 ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
global u32 const TABLE_PAGES = 100;
global u32 TABLE_ROWS = ROWS_PER_PAGE * TABLE_PAGES;


// NOTE: Here we are defining the layout of our data (format).
//          [(type)(is_root)(parent_pointer)][(num_cells)][(key)(value)(key)(value)(key)(value)]
//                          ^                      ^                         ^
//                 common header layout     leaf header layout         key value pairs
// NOTE: Common Node Header Layout. This is the layout that is common to all nodes, which contains the (type, is_root, parent_pointer).
global u32 NODE_TYPE_SIZE = sizeof(u8);
global u32 NODE_TYPE_OFFSET = 0;
global u32 IS_ROOT_SIZE = sizeof(u8);
global u32 IS_ROOT_OFFSET = NODE_TYPE_SIZE;
global u32 PARENT_POINTER_SIZE = sizeof(u32);
global u32 PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
global u8  COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// NOTE: Leaf Node Header Layout. This contains the layout of the leaf node, that proceeds the Common Node Header Layout. This will store just the number of cells the node contains.
global u32 LEAF_NODE_NUM_CELLS_SIZE = sizeof(u32);
global u32 LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
global u32 LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// NOTE: Leaf Node Body Layout. This is describing the data layout of our cells (key/value pair).
global u32 LEAF_NODE_KEY_SIZE = sizeof(u32);
global u32 LEAF_NODE_KEY_OFFSET = 0;
global u32 LEAF_NODE_VALUE_SIZE = ROW_SIZE;
global u32 LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
global u32 LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
global u32 LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
global u32 LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

global u32 LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
global u32 LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

// NOTE: Internal node header layout
global u32 INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(u32);
global u32 INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
global u32 INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(u32);
global u32 INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
global u32 INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// NOTE: Internal node body layout
global u32 INTERNAL_NODE_KEY_SIZE = sizeof(u32);
global u32 INTERNAL_NODE_CHILD_SIZE = sizeof(u32);
global u32 INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;

typedef enum NodeType{
    NodeType_internal,
    NodeType_leaf
} NodeType;

static NodeType
get_node_type(void* node){
    u8 result = *((u8*)node + NODE_TYPE_OFFSET);
    return((NodeType)result);
}

static void
set_node_root(void* node, bool is_root){
    *((u8*)node + IS_ROOT_OFFSET) = (u8)is_root;
}

static void
set_node_type(void* node, NodeType type){
    u8 type_u8 = type;
    *((u8*)node + NODE_TYPE_OFFSET) = type_u8;
}

static bool
is_node_root(void* node){
    u8 result = *(u8*)node + IS_ROOT_OFFSET;
    return((bool)result);
}

static u32*
internal_node_num_keys(void* node){
    return (u32*)((u8*)node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

static u32*
internal_node_right_child(void* node){
    return (u32*)((u8*)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

static u32*
internal_node_cell(void* node, u32 cell_num){
    return (u32*)((u8*)node + INTERNAL_NODE_HEADER_SIZE + (cell_num * INTERNAL_NODE_CELL_SIZE));
}

static u32*
internal_node_child(void* node, u32 child_num){
    u32 num_keys = *internal_node_num_keys(node);
    if(child_num > num_keys){
        print("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
        exit(EXIT_FAILURE);
    }
    else if(child_num == num_keys){
        return internal_node_right_child(node);
    }
    else{
        return internal_node_cell(node, child_num);
    }
}

static u32*
internal_node_key(void* node, u32 key_num){
    return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

static void
init_internal_node(void* node){
    set_node_type(node, NodeType_internal);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

static u32*
leaf_node_num_cells(void* node){
    u32* result = (u32*)((u8*)node + LEAF_NODE_NUM_CELLS_OFFSET);
    return(result);
}

static void*
leaf_node_cell(void* node, u32 cell_num){
    void* result = (u8*)node + LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE);
    return(result);
}

static u32*
leaf_node_key(void* node, u32 cell_num){
    u32* result = (u32*)leaf_node_cell(node, cell_num);
    return(result);
}

static void*
leaf_node_value(void* node, u32 cell_num){
    void* result = (u8*)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
    return(result);
}

static u32
get_node_max_key(void* node){
    switch(get_node_type(node)){
        case NodeType_internal:
            return *internal_node_key(node, *internal_node_num_keys(node) - 1);
        case NodeType_leaf:
            return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
}

static void
init_leaf_node(void* node){
    set_node_type(node, NodeType_leaf);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
}

typedef struct BTree{
    u8 type;
    bool is_root;
    struct BTree* parent;
} BTree;

typedef struct InputBuffer{
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef enum MetaCommand{
    MetaCommand_success,
    MetaCommand_unrecognized,
} MetaCommand;

typedef enum PrepareResult{
    PrepareResult_success,
    PrepareResult_unrecognized_statement,
    PrepareResult_syntax_error,
    PrepareResult_negative_id,
    PrepareResult_username_too_long,
    PrepareResult_email_too_long,
} PrepareResult;

typedef enum StatementType{
    StatementType_insert,
    StatementType_select,
} StatementType;

typedef enum ExecuteResult{
    ExecuteResult_success,
    ExecuteResult_table_full,
    ExecuteResult_duplicate_key,
} ExecuteResult;

typedef struct Statement{
    StatementType type;
    Row row;
    size_t size; // TODO: get rid of
} Statement;

typedef struct Table{
    u32 num_pages;
    u32 root_page_num;
    Arena* pages[TABLE_PAGES];
} Table;
global Table table;

static Arena*
get_page(Table* table, u32 page_num){
    Arena* result = table->pages[page_num];
    if(result == 0){
        result = os_alloc_arena(PAGE_SIZE);
        table->pages[page_num] = result;
    }
    //if(page_num >= table->num_pages){
    //    table->num_pages = page_num + 1;
    //}

    //if(result->used + ROW_SIZE > result->size){
    //    result = table->pages[table->num_pages++];
    //}
    //Arena* result = table->pages[page_num];
    if(page_num >= table->num_pages){
        table->num_pages = page_num + 1;
    }
    return(result);
}

typedef struct Cursor{
    Table* table;
    u32 page_num;
    u32 cell_num;
    bool end_of_table;
} Cursor;

static Cursor*
cursor_start(Table* table){
    Cursor* c = push_struct(tm, Cursor);
    c->table = table;
    c->page_num = table->root_page_num;
    c->cell_num = 0;

    Arena* root_node = get_page(table, table->root_page_num);
    u32 num_cells = *leaf_node_num_cells(root_node->base);
    c->end_of_table = (num_cells == 0);
    return(c);
}

static Cursor*
cursor_end(Table* table){
    Cursor* c = push_struct(tm, Cursor);
    c->table = table;
    c->page_num = table->root_page_num;
    Arena* root_node = get_page(table, table->root_page_num);
    c->cell_num = *leaf_node_num_cells(root_node->base);

    c->end_of_table = true;
    return(c);
}

static Cursor*
leaf_node_find(Table* table, u32 page_num, u32 key){
    Arena* node = get_page(table, page_num);
    u32 num_cells = *leaf_node_num_cells(node->base);

    Cursor* c = push_struct(tm, Cursor);
    c->table = table;
    c->page_num = page_num;

    // NOTE: binary search
    u32 min_index = 0;
    u32 opl_index = num_cells;
    while(min_index != opl_index){
        u32 index = (min_index + opl_index) / 2;
        u32 key_at_index = *leaf_node_key(node->base, index);
        if(key == key_at_index){
            c->cell_num = index;
            return(c);
        }
        if(key < key_at_index){
            opl_index = index;
        }
        else{
            min_index = index + 1;
        }
    }

    c->cell_num = min_index;
    return(c);
}

static Cursor*
internal_node_find(Table* table, u32 page_num, u32 key){
    void* node = get_page(table, page_num)->base;
    u32 num_keys = *internal_node_num_keys(node);

    // NOTE: Binary search to find index of child to search
    u32 min_index = 0;
    u32 max_index = num_keys;
    while(min_index != max_index){
        u32 index = (min_index + max_index) / 2;
        u32 key_to_right = *internal_node_key(node, index);
        if(key_to_right >= key){
            max_index = index;
        }
        else{
            min_index = index + 1;
        }
    }

    u32 child_num = *internal_node_child(node, min_index);
    void* child = get_page(table, child_num)->base;
    switch(get_node_type(child)){
        case NodeType_leaf:
            return(leaf_node_find(table, child_num, key));
        case NodeType_internal:
            return(internal_node_find(table, child_num, key));
    }
}

static Cursor*
cursor_find(Table* table, u32 key){
    u32 root_page_num = table->root_page_num;
    Arena* root_node = get_page(table, root_page_num);

    if((get_node_type(root_node->base) == NodeType_leaf)){
        return leaf_node_find(table, root_page_num, key);
    }
    else{
        //print("Need to implement searching an internal node.\n");
        //exit(EXIT_FAILURE);
        return(internal_node_find(table, root_page_num, key));
    }
}

static Row*
cursor_at(Cursor* c){
    Arena* page = get_page(c->table, c->page_num);
    Row* row = (Row*)leaf_node_value(page->base, c->cell_num);
    return(row);
}

static void
cursor_next(Cursor* c){
    Arena* page = get_page(c->table, c->page_num);
    c->cell_num += 1;
    if(c->cell_num >= (*leaf_node_num_cells(page->base))){
        c->end_of_table = true;
    }
}

static void
init_table(Table* table){
    table->num_pages = 0;
    table->root_page_num = 0;
    for(u32 i=0; i < TABLE_PAGES; ++i){
        table->pages[i] = 0;
    }
}

static void
print_constants() {
  print("ROW_SIZE: %d\n", ROW_SIZE);
  print("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  print("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  print("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  print("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  print("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

static void
indent(u32 level){
    for(u32 i=0; i < level; ++i){
        print("  ");
    }
}

static void
print_tree(Table* table, u32 page_num, u32 indentation_level){
    void* node = get_page(table, page_num)->base;
    u32 num_keys;
    u32 child;

    switch(get_node_type(node)){
        case NodeType_leaf:
            num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            print("- leaf (size %d)\n", num_keys);
            for(u32 i=0; i < num_keys; ++i){
                indent(indentation_level + 1);
                print("- %d\n", *leaf_node_key(node, i));
            }
            break;
        case NodeType_internal:
            num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            print("- internal (size %d)\n", num_keys);
            for(u32 i=0; i < num_keys; ++i){
                child = *internal_node_child(node, i);
                print_tree(table, child, indentation_level + 1);

                indent(indentation_level + 1);
                print("- key %d\n", *internal_node_key(node, i));
            }
            child = *internal_node_right_child(node);
            print_tree(table, child, indentation_level + 1);
            break;
    }
}

static void
print_leaf_node(void* node) {
  u32 num_cells = *leaf_node_num_cells(node);
  printf("leaf (size %d)\n", num_cells);
  for (u32 i = 0; i < num_cells; i++) {
    u32 key = *leaf_node_key(node, i);
    printf("  - %d : %d\n", i, key);
  }
}

static void
db_close(Table* table){
    u64 offset = 0;
    // IMPORTANT: potential memory access violation here if we are not skipping the last page.
    for(u32 i=0; i <= table->num_pages; ++i){
        //if(i == table->num_pages && row_offset == 0){
        //    continue;
        //}
        Arena* page = table->pages[i];
        if(page != 0){
            FileData data = {page->base, page->size};
            os_file_write(data, dir, filename, offset);
            offset += page->size;
        }
    }
}

static void
db_open(Arena* arena, Table* table){
    FileData file = os_file_read(arena, dir, filename);

    u32 remainder = file.size % PAGE_SIZE;
    if(remainder){
        print("db file is not a while number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    u32 num_pages = file.size / PAGE_SIZE;
    table->num_pages = num_pages;
    if(file.size == 0){
        // NOTE: new database. init page 0 as leaf node.
        Arena* page = os_alloc_arena(PAGE_SIZE);
        init_leaf_node(page->base);
        set_node_root(page->base, true);
        table->pages[0] = page;
    } else{
        for(u32 i=0; i <= num_pages; ++i){
            Arena* page = table->pages[i];

            page = os_alloc_arena(PAGE_SIZE);
            page->base = (u8*)file.base + (i * PAGE_SIZE);
            //page->used = ROWS_PER_PAGE * ROW_SIZE;
            table->pages[i] = page;
        }
        //table->pages[num_pages]->used = row_remainder * ROW_SIZE;
    }
}

static MetaCommand
do_meta_command(String8 input){
    if(input == str8_literal(".exit")){
        print("quiting");
        db_close(&table);
        running = false;
        exit(EXIT_SUCCESS);
        return(MetaCommand_success);
    }
    if(input == str8_literal(".constants")){
        print_constants();
        return(MetaCommand_success);
    }
    if(input == str8_literal(".btree")){
        print("Tree:\n");
        print_tree(&table, 0, 0);
        //print_leaf_node(get_page(&table, 0)->base);
        return(MetaCommand_success);
    }
    else{
        return(MetaCommand_unrecognized);
    }
}

static PrepareResult
prepare_insert(String8 input, Statement* statement){
    statement->type = StatementType_insert;

    char* keyword = strtok((char*)input.str, " ");
    char* id_string = strtok(0, " ");
    char* username = strtok(0, " ");
    char* email = strtok(0, " ");

    if(id_string == 0 || username == 0 || email == 0){
        return(PrepareResult_syntax_error);
    }

    u64 username_length = str_length(username);
    if(username_length > USERNAME_SIZE){
        return(PrepareResult_username_too_long);
    }
    u64 email_length = str_length(email);
    if(email_length > EMAIL_SIZE){
        return(PrepareResult_email_too_long);
    }
    s32 id = atoi(id_string); // NOTE: strtol is more flexible. consider using it later
    if(id < 0){
        return(PrepareResult_negative_id);
    }

    statement->row.id = (u32)id;
    strcpy(statement->row.username, username);
    strcpy(statement->row.email, email);

    return(PrepareResult_success);
}

static PrepareResult
prepare_statement(String8 input, Statement* statement){
    PrepareResult result = ZERO_INIT;
    if(str8_starts_with(input, str8_literal("insert"))){
        result = prepare_insert(input, statement);
        return(result);
    }

    if(str8_starts_with(input, str8_literal("select"))){
        statement->type = StatementType_select;
        return(PrepareResult_success);
    }

    return(PrepareResult_unrecognized_statement);
}

static void
serialize_row(void* dest, Row* row){
    memcpy((u8*)dest + ID_OFFSET, &row->id, ID_SIZE);
    memcpy((u8*)dest + USERNAME_OFFSET, row->username, USERNAME_SIZE);
    memcpy((u8*)dest + EMAIL_OFFSET, row->email, EMAIL_SIZE);
}

static void
deserialize_row(Row* row){
    print("(%d, %s, %s)\n", row->id, row->username, row->email);
}

static u32
get_unused_page_num(Table* table){
    return(table->num_pages);
}

static void
create_new_root(Table* table, u32 right_child_page_num){
    // NOTE: Handle splitting the root.
    // Old root copied to new page, becomes left child.
    // Address of rigth child passed in.
    // Re-initialize root page to contain the new root node.
    // New root node points to two children.
    void* root = get_page(table, table->root_page_num)->base;
    void* right_child = get_page(table, right_child_page_num)->base;
    u32 left_child_page_num = get_unused_page_num(table);
    void* left_child = get_page(table, left_child_page_num)->base;

    // NOTE: Left child has data copies from old root.
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    // NOTE: Root node is a new internal node with one key and two children.
    init_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    u32 left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
}

static void
leaf_node_split_and_insert(Cursor* c, u32 key, Row* row){
    // NOTE: Create a new node and move half the cells over.
    // Insert the value in one of the two noes.
    // Update parent or create a new parent.
    void* old_node = get_page(c->table, c->page_num)->base;
    u32 new_page_num = get_unused_page_num(c->table);
    void* new_node = get_page(c->table, new_page_num)->base;
    init_leaf_node(new_node);

    // NOTE: All existing keys plus new key should be divided evenly between old(left) and new(right) nodes.
    // Starting from the right, move each key to the correct position.
    for(s32 i=LEAF_NODE_MAX_CELLS; i >= 0; --i){
        void* dest_node;
        if(i >= LEAF_NODE_LEFT_SPLIT_COUNT){
            dest_node = new_node;
        }
        else{
            dest_node = old_node;
        }
        u32 index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* cell = leaf_node_cell(dest_node, index_within_node);

        if(i == c->cell_num){
            *leaf_node_key(dest_node, index_within_node) = key;
            serialize_row(leaf_node_value(dest_node, index_within_node), row);
            //serialize_row(cell, row);
        }
        else if(i > c->cell_num){
            memcpy(cell, leaf_node_cell(old_node, i-1), LEAF_NODE_CELL_SIZE);
        }
        else{
            memcpy(cell, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    // NOTE: Update cell count on both leaf nodes.
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    // NOTE: Update nodes' parent.
    if(is_node_root(old_node)){
        return(create_new_root(c->table, new_page_num));
    }
    else{
        print("Need to implement updating parent after split\n");
        exit(EXIT_FAILURE);
    }
}

static void
leaf_node_insert(Cursor* c, u32 key, Row* row){
    Arena* page = get_page(c->table, c->page_num);
    u32 num_cells = *leaf_node_num_cells(page->base);
    if(num_cells >= LEAF_NODE_MAX_CELLS){
        leaf_node_split_and_insert(c, key, row);
        return;
        //print("Need to implement splitting a leaf node.\n");
        //exit(EXIT_FAILURE);
    }

    if(c->cell_num < num_cells){
        for(s32 i=num_cells; i > c->cell_num; --i){
            memcpy(leaf_node_cell(page->base, i), leaf_node_cell(page->base, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(page->base)) += 1;
    *(leaf_node_key(page->base, c->cell_num)) = key;
    serialize_row(leaf_node_value(page->base, c->cell_num), row);
}

static ExecuteResult
execute_insert(Table* table, Statement* statement){
    Arena* node = get_page(table, table->root_page_num);
    u32 num_cells = *leaf_node_num_cells(node->base);
    //if(num_cells >= LEAF_NODE_MAX_CELLS){
    //    return(ExecuteResult_table_full);
    //}

    Row* row = &statement->row;
    u32 id = row->id;
    Cursor* c = cursor_find(table, id);

    if(c->cell_num < num_cells){
        u32 current_id = *leaf_node_key(node->base, c->cell_num);
        if(id == current_id){
            return(ExecuteResult_duplicate_key);
        }
    }
    leaf_node_insert(c, id, row);
    return(ExecuteResult_success);
}

static ExecuteResult
execute_select(Table* table, Statement* statement){
    Cursor* cursor = cursor_start(table);

    Row* at;
    while(!(cursor->end_of_table)){
        at = cursor_at(cursor);
        deserialize_row(at);
        cursor_next(cursor);
    }
    return(ExecuteResult_success);
}

static ExecuteResult
execute_statement(Table* table, Statement* statement){
    ExecuteResult result;
    switch(statement->type){
        case StatementType_insert:{
            result = execute_insert(table, statement);
        } break;
        case StatementType_select:{
            result = execute_select(table, statement);
        } break;
    }
    return(result);
}

static void
str8_strip_newline(String8* str){
    u8* ptr = str->str + str->size - 1;
    *ptr = 0;
    str->size -= 1;
}

s32 main(s32 argc, char** argv){
    //os_file_delete(dir, filename);
    init_table(&table);
    db_open(pm, &table);

    while(running){
        print("db > ");
        String8 input = read_stdin(tm);
        str8_strip_newline(&input);

        if(input.str[0] == '.'){
            MetaCommand command = do_meta_command(input);
            switch(command){
                case MetaCommand_success:{
                } continue;
                case MetaCommand_unrecognized:{
                    print("Unrecognized command: '%.*s'\n", (s32)input.size, input.str);
                } continue;
            }
        }

        Statement statement;
        PrepareResult prepare_result = prepare_statement(input, &statement);
        switch(prepare_result){
            case PrepareResult_success:{
            } break;
            case PrepareResult_syntax_error:{
                print("Sysntax error. Could not parse statement.\n");
            } continue;
            case PrepareResult_negative_id:{
                print("ID must be positive.\n");
            } continue;
            case PrepareResult_username_too_long:{
                print("Username string is too long. Max: %d\n", USERNAME_SIZE);
            } continue;
            case PrepareResult_email_too_long:{
                print("Email string is too long. Max: %d\n", EMAIL_SIZE);
            } continue;
            case PrepareResult_unrecognized_statement:{
                print("Unrecognized keyword: '%.*s'\n", (s32)input.size, input.str);
            } continue;
        }

        ExecuteResult execute_result = execute_statement(&table, &statement);
        switch(execute_result){
            case ExecuteResult_success:{
                print("Executed.\n");
            } break;
            case ExecuteResult_duplicate_key:{
                print("Error: Duplicate key.\n");
            } break;
            case ExecuteResult_table_full:{
                print("Error: Table full.\n");
            } break;
        }
        arena_free(tm);
    }
    return(0);
}
