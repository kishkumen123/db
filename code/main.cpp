#define _CRT_SECURE_NO_DEPRECATE 1

#include "base_inc.h"
#include "win32_base_inc.h"

global Arena* pm = alloc_arena(MB(4));
global Arena* tm = alloc_arena(MB(1));
global String8 dir = os_get_cwd(pm);
global String8 filename = str8_literal("\\data\\mydb.db");

typedef struct InputBuffer{
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef enum MetaCommand{
    MetaCommand_success,
    MetaCommand_unrecognized,
    MetaCommand_exit,
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
} ExecuteResult;

global u32 ID_SIZE = sizeof(s32);
global u32 const USERNAME_SIZE = 32;
global u32 const EMAIL_SIZE = 255;

typedef struct Row{
    s32 id;
    char username[USERNAME_SIZE];
    char email[EMAIL_SIZE];
} Row;

global u32 ROW_SIZE = sizeof(Row);
global u32 PAGE_SIZE = KB(1);
global u32 ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
global u32 const TABLE_PAGES = 100;
global u32 TABLE_ROWS = ROWS_PER_PAGE * TABLE_PAGES;


typedef struct Statement{
    StatementType type;
    Row row;
    size_t size; // TODO: get rid of
} Statement;

typedef struct Table{
    u32 num_rows;
    Arena* pages[TABLE_PAGES];
} Table;

typedef struct Cursor{
    Table* table;
    u32 row_num;
    bool end_of_table;
} Cursor;

static Cursor*
cursor_at_start(Table* table){
    Cursor* c = push_struct(tm, Cursor);
    c->table = table;
    c->row_num = 0;
    c->end_of_table = (table->num_rows == 0);
    return(c);
}

static Cursor*
cursor_at_end(Table* table){
    Cursor* c = push_struct(tm, Cursor);
    c->table = table;
    c->row_num = table->num_rows;
    c->end_of_table = true;
    return(c);
}

static void
cursor_next(Cursor* c){
    c->row_num += 1;
    if(c->row_num >= c->table->num_rows){
        c->end_of_table = true;
    }
}

static void
table_init(Table* table){
    for(u32 i=0; i < TABLE_PAGES; ++i){
        table->pages[i] = os_alloc_arena(PAGE_SIZE);
    }
    table->num_rows = 0;
}

static MetaCommand
do_meta_command(String8 input){
    if(input == str8_literal(".exit")){
        return(MetaCommand_exit);
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

    // TODO: Need to check to see if we are larger than the page size here
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

    statement->row.id = id;
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

static Arena*
get_next_page(Table* table){
    u32 num_rows = table->num_rows;
    u32 page_num = num_rows / ROWS_PER_PAGE;

    Arena* result = table->pages[page_num];
    return(result);
}

static Row*
cursor_at(Cursor* c){
    u32 row_num = c->row_num;
    u32 page_num = row_num / ROWS_PER_PAGE;
    u32 row_offset = row_num % ROWS_PER_PAGE;

    Arena* page = c->table->pages[page_num];
    Row* row = (Row*)((u8*)page->base + (row_offset * ROW_SIZE));
    return(row);
}

static void
serialize_row(Arena* page, Row row_data){
    Row* result = push_struct(page, Row);
    result->id = row_data.id;
    strcpy(result->username, row_data.username);
    strcpy(result->email, row_data.email);
}

//static void
//deserialize_page(Arena* page){
//    void* at = (u8*)page->base;
//    void* end = (u8*)page->base + page->used;
//
//    Row* row;
//    while(at != end){
//        row = (Row*)at;
//        print("(%d, %s, %s)\n", row->id, row->username, row->email);
//        at = (u8*)at + ROW_SIZE;
//    }
//}

static void
deserialize_row(Row* row){
    print("(%d, %s, %s)\n", row->id, row->username, row->email);
}

static ExecuteResult
execute_insert(Table* table, Statement* statement){
    u32 pages_filled = table->num_rows / ROWS_PER_PAGE;
    if(pages_filled >= TABLE_PAGES){
        return(ExecuteResult_table_full);
    }

    Arena* page = get_next_page(table);
    serialize_row(page, statement->row);
    table->num_rows++;
    print("page num: %d - page used: %d - page size: %d\n", pages_filled, page->used, page->size);
    return(ExecuteResult_success);
}

static ExecuteResult
execute_select(Table* table, Statement* statement){
    Cursor* cursor = cursor_at_start(table);

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

static void
db_close(Table* table){
    u32 pages_filled = table->num_rows / ROWS_PER_PAGE;
    u64 offset = 0;
    FileData num_rows = {&table->num_rows, sizeof(u32)};
	os_file_write(num_rows, dir, filename, 0);
    offset += sizeof(u32);
    for(u32 i=0; i <= pages_filled; ++i){
        Arena* page = table->pages[i];
        FileData data = {page->base, page->used};
        os_file_write(data, dir, filename, offset);
        offset += page->size;
    }
}

static void
db_open(Arena* arena, Table* table){
    FileData file = os_file_read(arena, dir, filename);
    if(file.size){
		table->num_rows = *(u32*)file.base;
		u32 pages_filled = table->num_rows / ROWS_PER_PAGE;
		u32 row_offset = table->num_rows % ROWS_PER_PAGE;
		void* at = (u8*)file.base + sizeof(u32);
        for(u32 i=0; i <= pages_filled; ++i){
            Arena* page = table->pages[i];
            page->base = (u8*)at + (i * PAGE_SIZE);

            if(i == pages_filled){
                page->used = row_offset * ROW_SIZE;
            }
            else{
                page->used = ROWS_PER_PAGE * ROW_SIZE;
            }
        }
    }
}

s32 main(s32 argc, char** argv){
    Table table;
    table_init(&table);
    db_open(pm, &table);

    bool running = true;
    while(running){
        print("db > ");
        String8 input = read_stdin(tm);
        str8_strip_newline(&input);

        if(input.str[0] == '.'){
            MetaCommand command = do_meta_command(input);
            switch(command){
                case MetaCommand_exit:{
                    print("quiting");
                    db_close(&table);
                    running = false;
                    exit(EXIT_SUCCESS);
                } continue;
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
            case ExecuteResult_table_full:{
                print("Error: Table full.\n");
            } break;
        }
        arena_free(tm);
    }
    return(0);
}
