#include"load.h"

using namespace std;

/**
 * Open the database
 * */
void openDB(const char* db_name, sqlite3*& db) {
    if( sqlite3_open(db_name, &db)){
        throw DBOperationException("Can’t open database\n");
    }
}

/**
 * Create all relations
 * */
void createTables(sqlite3* db) {
    if(sqlite3_exec(db, "PRAGMA foreign_keys = ON;", NULL, NULL, NULL)) {
        throw DBOperationException("Failed to enable foreign keys\n");
    }

    //Drop existing tables
    printf("dropping tables\n");
    int cnt = 0;
    const char* sql7 = "DROP TABLE IF EXISTS NUT_DATA";
    if(sqlite3_exec(db, sql7, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table nut_data\n");
    }
    const char* sql17 = "DROP TABLE IF EXISTS FOOTNOTE";
    if(sqlite3_exec(db, sql17, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table footnote\n");
    }
    const char* sql15 = "DROP TABLE IF EXISTS WEIGHT";
    if(sqlite3_exec(db, sql15, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table weight\n");
    }
    const char* sql5 = "DROP TABLE IF EXISTS LANGUAL;";
    if(sqlite3_exec(db, sql5, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table langual\n");
    }
    const char* sql5_2 = "DROP TABLE IF EXISTS LANGDESC;";
    if(sqlite3_exec(db, sql5_2, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table langdesc\n");
    }
    const char* sql19 = "DROP TABLE IF EXISTS DATSRCLN";
    if(sqlite3_exec(db, sql19, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table datasrcln\n");
    }
    const char* sql21 = "DROP TABLE IF EXISTS DATA_SRC";
    if(sqlite3_exec(db, sql21, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table data_src\n");
    }
    const char* sql11 = "DROP TABLE IF EXISTS SRC_CD";
    if(sqlite3_exec(db, sql11, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table src_cd\n");
    }
    const char* sql13 = "DROP TABLE IF EXISTS DERIV_CD";
    if(sqlite3_exec(db, sql13, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table deriv_cd\n");
    }

    const char* sql1 = "DROP TABLE IF EXISTS FOOD_DES;";
    if(sqlite3_exec(db, sql1, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table food_des\n");
    }
    const char* sql3 = "DROP TABLE IF EXISTS FD_GROUP;";
    if(sqlite3_exec(db, sql3, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table fd_group\n");
    }
    const char* sql9 = "DROP TABLE IF EXISTS NUTR_DEF";
    if(sqlite3_exec(db, sql9, NULL, NULL, NULL)) {
        throw DBOperationException("Failed to drop existing table nutr_def\n");
    }


    //Create new tables
    printf("creating tables\n");
    const char* sql2 = 
    "CREATE TABLE FOOD_DES("  
    "NDB_No         CHAR(5)     NOT NULL," 
    "FdGrp_Cd       CHAR(4)     NOT NULL," 
    "Long_Desc      CHAR(200)   NOT NULL," 
    "Shrt_Desc      CHAR(60)    NOT NULL," 
    "ComName        CHAR(100)   ," 
    "ManufacName    CHAR(65)    ," 
    "Survey         CHAR(1)     ," 
    "Ref_desc       CHAR(135)   ," 
    "Refuse         INTEGER     ," 
    "SciName        CHAR(65)    ," 
    "N_Factor       REAL        ," 
    "Pro_Factor     REAL        ," 
    "Fat_Factor     REAL        ," 
    "CHO_Factor     REAL        ,"
    "PRIMARY KEY ( NDB_No ),"
    "FOREIGN KEY ( FdGrp_Cd ) REFERENCES FD_GROUP ( FdGrp_Cd ) "
    ");";
    if(sqlite3_exec(db, sql2, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table FOOD_DES\n");
    }


    const char* sql4 = 
    "CREATE TABLE FD_GROUP("  
    "FdGrp_Cd       CHAR(4)     NOT NULL ," 
    "FdGrp_Desc     CHAR(60)    NOT NULL ," 
    "PRIMARY KEY ( FdGrp_Cd ) "
    ");";
    if(sqlite3_exec(db, sql4, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table FD_GROUP\n");
    }


    const char* sql6 = 
    "CREATE TABLE LANGUAL("  
    "NDB_No         CHAR(5)     NOT NULL," 
    "Factor_Code    CHAR(5)     NOT NULL,"
    "PRIMARY KEY ( NDB_No, Factor_Code ),"
    "FOREIGN KEY ( NDB_No ) REFERENCES FOOD_DES ( NDB_No ),"
    "FOREIGN KEY ( Factor_Code ) REFERENCES LANGDESC ( Factor_Code )"
    ");";
    if(sqlite3_exec(db, sql6, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table LANGUAL\n");
    }

    const char* sql6_2 = 
    "CREATE TABLE LANGDESC("  
    "Factor_Code    CHAR(5)     NOT NULL," 
    "Description    CHAR(140)   NOT NULL,"
    "PRIMARY KEY ( Factor_Code ) "
    ");";

    if(sqlite3_exec(db, sql6_2, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table LANGDESC\n");
    }


    const char* sql8 = 
    "CREATE TABLE NUT_DATA("  
    "NDB_No         CHAR(5)     NOT NULL," 
    "Nutr_No        CHAR(3)     NOT NULL,"
    "Nutr_Val       REAL        NOT NULL,"
    "Num_Data_Pts   REAL        NOT NULL,"
    "Std_Error      REAL        ,"
    "Src_Cd         CHAR(2)     NOT NULL,"
    "Deriv_Cd       CHAR(4)     ,"
    "Ref_NDB_No     CHAR(5)     ,"
    "Add_Nutr_Mark  CHAR(1)     ,"
    "Num_Studies    INTEGER     ,"
    "Min            REAL        ,"
    "Max            REAL        ,"
    "DF             INTEGER     ,"
    "Low_EB         REAL        ,"
    "Up_EB          REAL        ,"
    "Stat_cmt       CHAR(10)    ,"
    "AddMod_Date    CHAR(10)    ,"
    "CC             CHAR(1)     ,"
    "PRIMARY KEY ( NDB_No, Nutr_No),"
    "FOREIGN KEY ( Src_Cd ) REFERENCES SRC_CD ( Src_Cd ),"
    "FOREIGN KEY ( Deriv_Cd ) REFERENCES DERIV_CD ( Deriv_Cd ) "
    ");";


    if(sqlite3_exec(db, sql8, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table NUT_DATA\n");
    }


    const char* sql10 = 
    "CREATE TABLE NUTR_DEF("  
    "Nutr_No        CHAR(3)     NOT NULL,"
    "Units          CHAR(7)     NOT NULL,"
    "Tagname        CHAR(20)    ,"
    "NutrDesc       CHAR(60)    NOT NULL,"
    "Num_Dec        CHAR(1)     NOT NULL,"
    "SR_Order       INTEGER     NOT NULL,"
    "PRIMARY KEY ( Nutr_No ) "
    ");";


    if(sqlite3_exec(db, sql10, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table NUTR_DEF\n");
    }


    const char* sql12 = 
    "CREATE TABLE SRC_CD("  
    "Src_Cd         CHAR(2)     NOT NULL,"
    "SrcCd_Desc     CHAR(60)    NOT NULL,"
    "PRIMARY KEY ( Src_Cd ) "
    ");";

    if(sqlite3_exec(db, sql12, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table SRC_CD\n");
    }



    const char* sql14 = 
    "CREATE TABLE DERIV_CD("  
    "Deriv_Cd       CHAR(4)     NOT NULL,"
    "Deriv_Desc     CHAR(120)   NOT NULL,"
    "PRIMARY KEY ( Deriv_Cd ) "
    ");";
    if(sqlite3_exec(db, sql14, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table DERIV_CD\n");
    }



    const char* sql16 = 
    "CREATE TABLE WEIGHT("  
    "NDB_No         CHAR(5)     NOT NULL,"
    "Seq            CHAR(2)     NOT NULL,"
    "Amount         REAL        NOT NULL,"
    "Msre_Desc      CHAR(84)    NOT NULL,"
    "Gm_Wgt         REAL        NOT NULL,"
    "Num_Data_Pts   INTEGER     ,"
    "Std_Dev        REAL        ,"
    "PRIMARY KEY ( NDB_No, Seq ),"
    "FOREIGN KEY ( NDB_No ) REFERENCES FOOD_DES ( NDB_No ) "
    ");";

    if(sqlite3_exec(db, sql16, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table WEIGHT\n");
    }


    const char* sql18 = 
    "CREATE TABLE FOOTNOTE("  
    "NDB_No         CHAR(5)     NOT NULL,"
    "Footnt_No      CHAR(4)     NOT NULL,"
    "Footnt_Typ     CHAR(1)     NOT NULL,"
    "Nutr_No        CHAR(3)     ,"
    "Footnt_Txt     CHAR(200)   NOT NULL,"
    "FOREIGN KEY ( NDB_No ) REFERENCES FOOD_DES ( NDB_No ),"
    "FOREIGN KEY ( Nutr_No ) REFERENCES NUTR_DEF ( Nutr_No ) "
    ");" ;

    if(sqlite3_exec(db, sql18, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table FOOTNOTE\n");
    }


    const char* sql20 = 
    "CREATE TABLE DATSRCLN("  
    "NDB_No         CHAR(5)     NOT NULL,"
    "Nutr_No        CHAR(3)     NOT NULL,"
    "DataSrc_ID     CHAR(6)     NOT NULL,"
    "PRIMARY KEY ( NDB_No, Nutr_No, DataSrc_ID),"
    "FOREIGN KEY ( NDB_No ) REFERENCES FOOD_DES ( NDB_No ),"
    "FOREIGN KEY ( DataSrc_ID ) REFERENCES DATA_SRC ( DataSrc_ID ) "
    ");";
    if(sqlite3_exec(db, sql20, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table DATSRCLN\n");
    }


    const char* sql22 = 
    "CREATE TABLE DATA_SRC("  
    "DataSrc_ID     CHAR(6)     NOT NULL,"
    "Authors        CHAR(255)   ,"
    "Title          CHAR(255)   NOT NULL,"
    "Year           CHAR(4)     ,"
    "Journal        CHAR(135)   ,"
    "Vol_City       CHAR(16)    ,"
    "Issue_State    CHAR(5)     ,"
    "Start_Page     CHAR(5)     ,"
    "End_Page       CHAR(5)     ,"
    "PRIMARY KEY ( DataSrc_ID)  "
    ");";
    if(sqlite3_exec(db, sql22, NULL, NULL, NULL)) {
        throw DBOperationException("Can’t create table DATA_SRC\n");
    }
}



/**
 * Read data from dataset and insert into DB
 * */
void insertData(sqlite3* db, const int fieldNum, 
        const string& relationName, const char* fieldNames[]) {

    string relationFileName = relationName + ".txt";
    ifstream infile(relationFileName.c_str());

    string line;
    int cnt = 0;
    while(getline(infile, line)) {
        line.erase(line.length()-1);
        cnt ++;

        vector<string> data = split(line, '^');

        if(data.size() > fieldNum) {
            throw DBOperationException("Data has more fields than expected\n");
        }

        string fieldStr = "", valStr = "";
        bool started = false;
        for(size_t i = 0; i < data.size(); i ++) {
            string separator = "";
            if(started) separator = ", ";
            fieldStr += separator + fieldNames[i];
            if(data[i].length() == 0 || data[i] == "''"){
                valStr += separator + "NULL";
            }
            else {
                valStr += separator + data[i];
            }
            started = true;
        }
        if(started == false) continue;  //empty record

        string sql = "INSERT INTO "+relationName+"( ";
        sql += fieldStr;
        sql += " ) VALUES ( ";
        sql += valStr;
        sql += " )";

        if(sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL)) {
            cout<<fieldStr<<endl;
            cout<<valStr<<endl;
            throw DBOperationException("Faild to insert new data\n");
        }
    }

}

void insertFOOD_DES(sqlite3* db) {
    const int fieldNum = 14;
    const char* fieldNames[fieldNum] = {"NDB_No", 
        "FdGrp_Cd", "Long_Desc", "Shrt_Desc", "ComName", "ManufacName", "Survey",
        "Ref_desc", "Refuse", "SciName", "N_Factor", "Pro_Factor", "Fat_Factor", 
        "CHO_Factor"};

    insertData(db, fieldNum, "FOOD_DES", fieldNames);
}

void insertFD_GROUP(sqlite3* db) {
    const int fieldNum = 2;
    const char* fieldNames[fieldNum] = {"FdGrp_Cd", "FdGrp_Desc"};

    insertData(db, fieldNum, "FD_GROUP", fieldNames);
}

void insertLANGUAL(sqlite3* db) {
    const int fieldNum = 2;
    const char* fieldNames[fieldNum] = {"NDB_No", "Factor_Code"};

    insertData(db, fieldNum, "LANGUAL", fieldNames);
}

void insertLANGDESC(sqlite3* db) {
    const int fieldNum = 2;
    const char* fieldNames[fieldNum] = {"Factor_Code", "Description"};

    insertData(db, fieldNum, "LANGDESC", fieldNames);
}

void insertNUT_DATA(sqlite3* db) {
    const int fieldNum = 18;
    const char* fieldNames[fieldNum] = 
        {"NDB_No","Nutr_No","Nutr_Val","Num_Data_Pts", "Std_Error",    
        "Src_Cd","Deriv_Cd","Ref_NDB_No","Add_Nutr_Mark",
        "Num_Studies","Min","Max","DF","Low_EB",       
        "Up_EB","Stat_cmt","AddMod_Date","CC"};

    insertData(db, fieldNum, "NUT_DATA", fieldNames);
}

void insertNUTR_DEF(sqlite3* db) {
    const int fieldNum = 6;
    const char* fieldNames[fieldNum] = 
        {"Nutr_No","Units","Tagname", "NutrDesc",    
        "Num_Dec","SR_Order"};

    insertData(db, fieldNum, "NUTR_DEF", fieldNames);
}

void insertSRC_CD(sqlite3* db) {
    const int fieldNum = 2;
    const char* fieldNames[fieldNum] = 
        {"Src_Cd","SrcCd_Desc"};

    insertData(db, fieldNum, "SRC_CD", fieldNames);
}

void insertDERIV_CD(sqlite3* db) {
    const int fieldNum = 2;
    const char* fieldNames[fieldNum] = 
        {"Deriv_Cd","Deriv_Desc"};

    insertData(db, fieldNum, "DERIV_CD", fieldNames);
}

void insertWEIGHT(sqlite3* db) {
    const int fieldNum = 7;
    const char* fieldNames[fieldNum] = 
        {"NDB_No","Seq","Amount","Msre_Desc","Gm_Wgt","Num_Data_Pts","Std_Dev"};

    insertData(db, fieldNum, "WEIGHT", fieldNames);
}

void insertFOOTNOTE(sqlite3* db) {
    const int fieldNum = 5;
    const char* fieldNames[fieldNum] = 
        {"NDB_No","Footnt_No","Footnt_Typ","Nutr_No","Footnt_Txt"};

    insertData(db, fieldNum, "FOOTNOTE", fieldNames);
}

void insertDATSRCLN(sqlite3* db) {
    const int fieldNum = 3;
    const char* fieldNames[fieldNum] = 
        {"NDB_No","Nutr_No","DataSrc_ID"};

    insertData(db, fieldNum, "DATSRCLN", fieldNames);
}
void insertDATA_SRC(sqlite3* db) {
    const int fieldNum = 9;
    const char* fieldNames[fieldNum] = 
        { "DataSrc_ID","Authors","Title",
            "Year","Journal","Vol_City",
            "Issue_State","Start_Page","End_Page" };

    insertData(db, fieldNum, "DATA_SRC", fieldNames);
}

int main() {

    sqlite3 *db;

    try {
        openDB("nutrients.db", db);

        createTables(db);

        //Begin a transaction
        if(sqlite3_exec(db, "BEGIN; ", NULL, NULL, NULL)) {
            throw DBOperationException("Faild to begin transaction\n");
        }

        cout<<"inserting food group\n";
        insertFD_GROUP(db);

        cout<<"inserting food des\n";
        insertFOOD_DES(db);

        cout<<"inserting nut def\n";
        insertNUTR_DEF(db);

        cout<<"inserting lang desc\n";
        insertLANGDESC(db);

        cout<<"inserting deriv cd\n";
        insertDERIV_CD(db);

        cout<<"inserting wgt\n";
        insertWEIGHT(db);

        cout<<"inserting data src\n";
        insertDATA_SRC(db);

        cout<<"inserting dat src ln\n";
        insertDATSRCLN(db);

        cout<<"inserting src cd\n";
        insertSRC_CD(db);

        cout<<"inserting lang\n";
        insertLANGUAL(db);

        cout<<"inserting nut data\n";
        insertNUT_DATA(db);

        cout<<"inserting ftnt\n";
        insertFOOTNOTE(db);

        //Commit the transaction
        if(sqlite3_exec(db, "COMMIT; ", NULL, NULL, NULL)) {
            throw DBOperationException("Faild to commit\n");
        }

        sqlite3_close(db);

    } catch(DBOperationException ex) {
        fprintf(stderr, "%s\n", ex.msg);
        fprintf(stderr, "%s\n", sqlite3_errmsg(db));
        return 1;
    }

    return 0;
}
