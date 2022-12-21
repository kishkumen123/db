#define _CRT_SECURE_NO_DEPRECATE 1

#include "base_inc.h"
#include "win32_base_inc.h"

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
} PrepareResult;

typedef enum StatementType{
    StatementType_insert,
    StatementType_select,
} StatementType;

typedef enum ExecuteResult{
    ExecuteResult_success,
    ExecuteResult_table_full,
} ExecuteResult;

typedef struct Row{
    u32 id;
    String8 username;
    String8 email;
    size_t arena_used;
    //size_t arena_prev_used;
} Row;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
//global u32 ID_SIZE = size_of_attribute(Row, id);
//global u32 USERNAME_SIZE = size_of_attribute(Row, id);
//global u32 EMAIL_SIZE = size_of_attribute(Row, id);

global u32 USERNAME_MAX_SIZE = 32;
global u32 EMAIL_MAX_SIZE = 255;
global u32 ROW_MAX_SIZE = sizeof(Row) + USERNAME_MAX_SIZE + EMAIL_MAX_SIZE;
global u32 WORD_MAX_SIZE = 1024;

//u32 ID_OFFSET = 0;
//u32 USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
//u32 EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

u32 PAGE_SIZE = KB(1);
#define TABLE_MAX_PAGES 100
//u32 ROWS_PER_PAGE = PAGE_SIZE / ROW_MAX_SIZE;
//u32 TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;


typedef struct Statement{
    StatementType type;
    Row row;
    size_t size;
} Statement;

typedef struct Table{
    u32 pages_filled;
    Arena* pages[TABLE_MAX_PAGES];

} Table;

static void
init_table(Table* table){
    for(u32 i=0; i < TABLE_MAX_PAGES; ++i){
        table->pages[i] = os_allocate_arena(PAGE_SIZE);
    }
    table->pages_filled = 0;
}

// TODO: fgets() adds \n character. String8 needs to be stronger to handle this.
// study Allens Websters string stuff
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
prepare_statement(Arena* arena, String8 input, Statement* statement){
    if(str8_starts_with(input, str8_literal("insert"))){
        statement->type = StatementType_insert;

        ScratchArena scratch = begin_scratch(0);
        u8* username_str = push_array(scratch.arena, u8, WORD_MAX_SIZE);
        u8* email_str = push_array(scratch.arena, u8, WORD_MAX_SIZE);
        s32 args = sscanf((char*)input.str, "insert %d %s %s", &(statement->row.id), (char*)username_str, email_str);
        u64 username_length = str_length((char*)username_str);
        u64 email_length = str_length((char*)email_str);

        // NOTE: + 1 on push_array() to account for 0 terminator
        statement->row.username.str = push_array(arena, u8, username_length + 1);
        statement->row.email.str = push_array(arena, u8, email_length + 1);
        memcpy(statement->row.username.str, username_str, username_length);
        memcpy(statement->row.email.str, email_str, email_length);
        statement->row.username.size = username_length;
        statement->row.email.size = email_length;
        size_t unaligned_size = sizeof(Row) + username_length + email_length + 2;
        statement->size = AlignUpPow2(unaligned_size, sizeof(Row));
        end_scratch(scratch);

        //statement->row.username.str = push_array(arena, u8, USERNAME_MAX_SIZE);
        //statement->row.email.str = push_array(arena, u8, EMAIL_MAX_SIZE);
        //s32 args = sscanf((char*)input.str, "insert %d %s %s", &(statement->row.id), (char*)statement->row.username.str, (char*)statement->row.email.str);
        //statement->row.username.size = str_length((char*)statement->row.username.str);
        //statement->row.email.size = str_length((char*)statement->row.email.str);

        //if(statement->row.username.size > 32){
        //    statement->row.username.str[32] = '\0';
        //    statement->row.username.size = 32;
        //}
        //if(statement->row.email.size >= 255){
        //    statement->row.username.str[32] = '\0';
        //}

        if(args < 3){
            return(PrepareResult_syntax_error);
        }
        return(PrepareResult_success);
    }
    if(str8_starts_with(input, str8_literal("select"))){
        statement->type = StatementType_select;
        return(PrepareResult_success);
    }

    return(PrepareResult_unrecognized_statement);
}

static Arena*
get_next_page(Table* table, Statement* statment){
    Arena* page = table->pages[table->pages_filled];
    //if(page->used + 64 > page->size){
    if(page->used + statment->size > page->size){
        table->pages_filled++;
        page = table->pages[table->pages_filled];
    }
    return(page);
}

static Row*
serialize_row(Arena* page, Row row_data){
    Row* result = push_struct(page, Row);
    // NOTE: + 1 on push_array to account for 0 terminator
    result->username.str = push_array(page, u8, row_data.username.size + 1);
    result->email.str = push_array(page, u8, row_data.email.size + 1);
    result->id = row_data.id;
    result->username.size = row_data.username.size;
    result->email.size = row_data.email.size;

    memcpy(result->username.str, row_data.username.str, row_data.username.size);
    memcpy(result->email.str, row_data.email.str, row_data.email.size);

    // NOTE: align on size of Row
    page->used = AlignUpPow2(page->used, sizeof(Row));
    result->arena_used = page->used;

    return(result);
}

static void deserialize_row(Arena* page){
    void* at = page->base;
    void* end = (u8*)page->base + page->used;

    Row* row;
    while(at != end){
        row = (Row*)at;
        print("(%d, %s, %s)\n", row->id, row->username.str, row->email.str);
        at = (u8*)page->base + row->arena_used;
    }
}

static ExecuteResult
execute_insert(Table* table, Statement* statement){
    if(table->pages_filled >= TABLE_MAX_PAGES){
        return(ExecuteResult_table_full);
    }

    Arena* page = get_next_page(table, statement);
    Row* row = serialize_row(page, statement->row);
    print("page num: %d - page used: %d - page size: %d\n", table->pages_filled, page->used, page->size);
    return(ExecuteResult_success);
}

static ExecuteResult
execute_select(Table* table, Statement* statement){
    for(u32 i=0; i <= table->pages_filled; ++i){
        Arena* page = table->pages[i];
        deserialize_row(page);
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
    Arena* string_arena = allocate_arena(MB(1));
    //Arena* keyword_arena = allocate_arena(MB(1));
    Table table;
    init_table(&table);

    bool running = true;
    while(running){
        print("db > ");
        String8 input = read_stdin(string_arena);
        str8_strip_newline(&input);

        if(input.str[0] == '.'){
            MetaCommand command = do_meta_command(input);
            switch(command){
                case MetaCommand_exit:{
                    print("quiting");
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
        PrepareResult prepare_result = prepare_statement(string_arena, input, &statement);
        switch(prepare_result){
            case PrepareResult_success:{
            } break;
            case PrepareResult_syntax_error:{
                print("Sysntax error. Could not parse statement.\n");
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
        arena_free(string_arena);
    }
    return(0);
}
