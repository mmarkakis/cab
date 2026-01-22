import SnowflakePool from './snowflake_pool.js';
import AthenaPool from "./athena_pool.js";
import BigQueryPool from "./bigquery_pool.js";
import RedshiftPool from "./redshift_pool.js";
import Common from './common.js';
import chalk from 'chalk';
import fs from "fs";

const gb_per_chunk = 1;
const snowflake_pool_concurrency = 5;
const athena_pool_concurrency = 5;
const big_query_pool_concurrency = 5;
const redshift_pool_concurrency= 5;

class LoadSetupManager {
   constructor() {
      this.query_streams = [];
      // this.snowflake = new SnowflakePool(snowflake_pool_concurrency, false);
      // this.athena = new AthenaPool(athena_pool_concurrency, false);
      this.redshift = new RedshiftPool(redshift_pool_concurrency, false)
      // this.big_query_pool = new BigQueryPool(big_query_pool_concurrency, false);
   }

   // Read query_streams from disk and gather meta info for all databases
   LoadDatabases(path) {
      this.query_streams = Common.LoadDatabaseMetaInfo(path);
      if (this.query_streams.length === 0) {
         console.log(chalk.red("No query streams were found, make sure the first one is query_stream_0.json"));
         process.exit(0);
      }
   }

   // Create tables for all scale_factors
   async CreateDataTablesSnowflake() {
      console.log(chalk.cyan("\nCreating snowflake tables ..."));
      for (const query_stream of this.query_streams) {
         this.snowflake.RunPrintSync("create or replace TABLE REGION_" + query_stream.database_id + " ( R_REGIONKEY NUMBER(38,0), R_NAME VARCHAR(25), R_COMMENT VARCHAR(152) );");
         this.snowflake.RunPrintSync("create or replace TABLE NATION_" + query_stream.database_id + " ( N_NATIONKEY NUMBER(38,0), N_NAME VARCHAR(25), N_REGIONKEY NUMBER(38,0), N_COMMENT VARCHAR(152) );");
         this.snowflake.RunPrintSync("create or replace TABLE CUSTOMER_" + query_stream.database_id + " ( C_CUSTKEY NUMBER(38,0), C_NAME VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY NUMBER(38,0), C_PHONE VARCHAR(15), C_ACCTBAL NUMBER(12,2), C_MKTSEGMENT VARCHAR(10), C_COMMENT VARCHAR(117));");
         this.snowflake.RunPrintSync("create or replace TABLE LINEITEM_" + query_stream.database_id + " ( L_ORDERKEY NUMBER(38,0), L_PARTKEY NUMBER(38,0), L_SUPPKEY NUMBER(38,0), L_LINENUMBER NUMBER(38,0), L_QUANTITY NUMBER(12,2), L_EXTENDEDPRICE NUMBER(12,2), L_DISCOUNT NUMBER(12,2), L_TAX NUMBER(12,2), L_RETURNFLAG VARCHAR(1), L_LINESTATUS VARCHAR(1), L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT VARCHAR(25), L_SHIPMODE VARCHAR(10), L_COMMENT VARCHAR(44));");
         this.snowflake.RunPrintSync("create or replace TABLE ORDERS_" + query_stream.database_id + " ( O_ORDERKEY NUMBER(38,0), O_CUSTKEY NUMBER(38,0), O_ORDERSTATUS VARCHAR(1), O_TOTALPRICE NUMBER(12,2), O_ORDERDATE DATE, O_ORDERPRIORITY VARCHAR(15), O_CLERK VARCHAR(15), O_SHIPPRIORITY NUMBER(38,0), O_COMMENT VARCHAR(79));");
         this.snowflake.RunPrintSync("create or replace TABLE PART_" + query_stream.database_id + " ( P_PARTKEY NUMBER(38,0), P_NAME VARCHAR(55), P_MFGR VARCHAR(25), P_BRAND VARCHAR(10), P_TYPE VARCHAR(25), P_SIZE NUMBER(38,0), P_CONTAINER VARCHAR(10), P_RETAILPRICE NUMBER(12,2), P_COMMENT VARCHAR(23));");
         this.snowflake.RunPrintSync("create or replace TABLE PARTSUPP_" + query_stream.database_id + " ( PS_PARTKEY NUMBER(38,0), PS_SUPPKEY NUMBER(38,0), PS_AVAILQTY NUMBER(38,0), PS_SUPPLYCOST NUMBER(12,2), PS_COMMENT VARCHAR(199) );");
         this.snowflake.RunPrintSync("create or replace TABLE SUPPLIER_" + query_stream.database_id + " ( S_SUPPKEY NUMBER(38,0), S_NAME VARCHAR(25), S_ADDRESS VARCHAR(40), S_NATIONKEY NUMBER(38,0), S_PHONE VARCHAR(15), S_ACCTBAL NUMBER(12,2), S_COMMENT VARCHAR(101) );");
         await this.snowflake.Wait();
      }
   }

   // Create tables for all scale_factors
   async CreateDataTablesAthena() {
      console.log(chalk.cyan("\nCreating athena tables ..."));
      for (const file of ["sql_athena/drop_table_ddl.sql", "sql_athena/create_table_ddl.sql"]) {
         const table_ddl_template = fs.readFileSync(file).toString();
         for (const query_stream of this.query_streams) {
            const table_ddls = table_ddl_template.replaceAll(":database_id:", query_stream.database_id).replaceAll("\n", "").split(":split:");
            for (const table_ddl of table_ddls) {
               console.log(table_ddl.substr(0, table_ddl.indexOf('(') === -1 ? table_ddl.indexOf(';') : table_ddl.indexOf('(')) + ";");
               this.athena.RunSync(table_ddl);
            }
            await this.athena.Wait();
         }
      }
   }

   // Create tables for all scale_factors
   async CreateDataTablesBigQuery() {
      console.log(chalk.cyan("\nCreating bigquery tables ..."));
      const table_ddl_template = fs.readFileSync("sql_big_query/create_table_ddl.sql").toString();
      for (const query_stream of this.query_streams) {
         const table_ddls = table_ddl_template.replaceAll(":database_id:", query_stream.database_id).replaceAll(":dataset_name:", this.big_query_pool.connection_options.dataset_name).replaceAll("\n", "").split(":split:");
         for (const table_ddl of table_ddls) {
            console.log(table_ddl.substr(0, table_ddl.indexOf('(') === -1 ? table_ddl.indexOf(';') : table_ddl.indexOf('(')) + ";");
            await this.big_query_pool.RunSync(table_ddl);
         }
         await this.big_query_pool.Wait();
      }
   }

   async CreateDataTablesRedshift() {
      console.log(chalk.cyan("\nCreating redshift tables ..."));
      for (const file of ["sql_redshift/drop_table_ddl.sql", "sql_redshift/create_table_ddl.sql"]) {
         console.log(file);
         const table_ddl_template = fs.readFileSync(file).toString();
         for (const query_stream of this.query_streams) {
            const table_ddls = table_ddl_template.replaceAll(":database_id:", query_stream.database_id).replaceAll("\n", "").split(":split:");
            for (const table_ddl of table_ddls) {
               console.log(table_ddl.substr(0, table_ddl.indexOf('(') === -1 ? table_ddl.indexOf(';') : table_ddl.indexOf('(')) + ";");
               this.redshift.RunSync(table_ddl);
            }
            await this.redshift.Wait();
            
         }
      }
   }
   

   async CreateJobTables() {
      console.log(chalk.cyan("\nCreating load jobs ..."));

      await this.snowflake.RunPrintSync("create or replace table Jobs(job_id int, database_id int, scale_factor int, table_name string, chunk_count int, step int, status string);");
      await this.snowflake.Wait();
   }

   async CreateJobTablesAthena() {
      console.log(chalk.cyan("\nCreating load jobs ..."));

      await this.athena.RunPrintSync("create table jobs(job_id int, database_id int, scale_factor int, table_name string, chunk_count int, step int, status string) LOCATION 's3://cloudglide/jobs' TBLPROPERTIES ('table_type'='ICEBERG');");
      await this.athena.Wait();
   }

   async CreateJobTablesRedshift() {
      console.log(chalk.cyan("\nCreating load jobs ..."));

      await this.redshift.RunPrintSync("create table jobs(job_id int, database_id int, scale_factor int, table_name varchar(152), chunk_count int, step int, status varchar(152));");
      await this.redshift.Wait();
   }
   
   async CreateLoadJobs() {
      let job_id = 1;

      const split_table_into_chunks = (database_id, scale_factor, gb_per_scale_factor, relation_name) => {
         const relation_gb = (scale_factor * gb_per_scale_factor);
         const relation_chunks = Math.ceil(relation_gb / gb_per_chunk);
         return Array.from(Array(relation_chunks).keys()).map(k => [job_id++, database_id, scale_factor, relation_name, relation_chunks, k + 1, 'open']);
      }

      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'region', 1, 1, 'open']));
      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'nation', 1, 1, 'open']));
      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'customer')).flat());
      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.725, 'lineitem')).flat());
      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.164, 'orders')).flat());
      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'part')).flat());
      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.113, 'partsupp')).flat());
      this.athena.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.001, 'supplier')).flat());
      await this.athena.Wait();
   }
   
   async CreateLoadJobsAthena() {
      let job_id = 1;

      const split_table_into_chunks = (database_id, scale_factor, gb_per_scale_factor, relation_name) => {
          const relation_gb = (scale_factor * gb_per_scale_factor);
          const relation_chunks = Math.ceil(relation_gb / gb_per_chunk);
          return Array.from(Array(relation_chunks).keys()).map(k => [job_id++, database_id, scale_factor, relation_name, relation_chunks, k + 1, 'open']);
      }
  
    // Function to generate SQL insert statements
    const generateInsertStatements = (query_streams, chunksFunction, ...params) => {
      return query_streams.map(db => chunksFunction(db.database_id, db.scale_factor, ...params)).flat().map(row => {
          // Correct quoting for STRING values
          return `INSERT INTO jobs (job_id, database_id, scale_factor, table_name, chunk_count, step, status) VALUES (${row.map((v, i) => i === 3 || i === 6 ? `'${v}'` : v).join(', ')});`;
      });
   }
  
      // Function to execute each SQL statement one at a time
      const executeInsertStatements = async (insertStatements) => {
          for (const statement of insertStatements) {
              try {
                  console.log(`Executing: ${statement}`); // For debugging
                  await this.athena.RunPrintSync(statement);
              } catch (error) {
                  console.error(`Error executing statement: ${statement}`, error);
                  throw error; // Optionally handle or log errors more specifically
              }
          }
      }
  
      // Generate insert statements
      const insertStatements1 = generateInsertStatements(this.query_streams, (database_id, scale_factor) => [[job_id++, database_id, scale_factor, 'region', 1, 1, 'open']]);
      const insertStatements2 = generateInsertStatements(this.query_streams, (database_id, scale_factor) => [[job_id++, database_id, scale_factor, 'nation', 1, 1, 'open']]);
      const insertStatements3 = generateInsertStatements(this.query_streams, split_table_into_chunks, 0.023, 'customer');
      const insertStatements4 = generateInsertStatements(this.query_streams, split_table_into_chunks, 0.725, 'lineitem');
      const insertStatements5 = generateInsertStatements(this.query_streams, split_table_into_chunks, 0.164, 'orders');
      const insertStatements6 = generateInsertStatements(this.query_streams, split_table_into_chunks, 0.023, 'part');
      const insertStatements7 = generateInsertStatements(this.query_streams, split_table_into_chunks, 0.113, 'partsupp');
      const insertStatements8 = generateInsertStatements(this.query_streams, split_table_into_chunks, 0.001, 'supplier');
  
      // Execute each set of insert statements individually
      await executeInsertStatements(insertStatements1);
      await executeInsertStatements(insertStatements2);
      await executeInsertStatements(insertStatements3);
      await executeInsertStatements(insertStatements4);
      await executeInsertStatements(insertStatements5);
      await executeInsertStatements(insertStatements6);
      await executeInsertStatements(insertStatements7);
      await executeInsertStatements(insertStatements8);
  
      await this.athena.Wait();
  }

   // Inserts "load-tasks" into the Jobs table. Each task populates a chunk of a table
   async CreateLoadJobsRedshift() {
      let job_id = 1;
      const split_table_into_chunks = (database_id, scale_factor, gb_per_scale_factor, relation_name) => {
         const relation_gb = (scale_factor * gb_per_scale_factor);
         const relation_chunks = Math.ceil(relation_gb / gb_per_chunk);
         return Array.from(Array(relation_chunks).keys()).map(k => [job_id++, database_id, scale_factor, relation_name, relation_chunks, k + 1, 'open']);
      }
      
      for (let params of this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'region', 1, 1, 'open'])) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
       }
      for (let params of this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'nation', 1, 1, 'open'])) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
      }       
      for (let params of this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'customer')).flat()) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
      }
      for (let params of this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.725, 'lineitem')).flat()) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
      }
      for (let params of this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.164, 'orders')).flat()) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
      }
      for (let params of this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'part')).flat()) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
      }
      for (let params of this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.113, 'partsupp')).flat()) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
      }
      for (let params of this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.001, 'supplier')).flat()) {
         this.athena.RunPrintSync("insert into jobs values($1, $2, $3, $4, $5, $6, $7);", params);
      }

      await this.athena.Wait();
   }
}

async function main() {
   await Common.ConfirmRun();

   const setup_manager = new LoadSetupManager();
   setup_manager.LoadDatabases("query_streams");
   // await setup_manager.CreateDataTablesSnowflake();
   // await setup_manager.CreateDataTablesAthena();
   // await setup_manager.CreateDataTablesBigQuery();
   await setup_manager.CreateDataTablesRedshift();
   // await setup_manager.CreateJobTablesAthena();
   await setup_manager.CreateLoadJobs();
   console.log(chalk.cyan("\nNormal program exit: setup " + setup_manager.query_streams.length + " databases :)"));
}

main();
