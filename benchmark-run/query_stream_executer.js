import SnowflakePool from './snowflake_pool.js';
import AthenaPool from './athena_pool.js';
import BigQueryPool from './bigquery_pool.js';
import RedshiftPool from './redshift_pool.js'
import fs from "fs";
import chalk from 'chalk';

import path, { dirname } from "path";
import { fileURLToPath } from "url";

// Reconstruct __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Define database concurrency level
const database_concurrency = 10;

class QueryStreamExecuter {
   constructor(database_config) {
      this.database_config = database_config;
      this.database_connection = new database_config.connector(database_concurrency, false);
      this.query_execution_log = [];
      this.total_cost = 0;
      this.total_scanned = 0;
   }

   // load query stream from a JSON file
   async LoadQueryStream(stream_id) {
      this.database = JSON.parse(fs.readFileSync("query_streams/query_stream_" + stream_id + ".json"))
      const meta = Object.fromEntries(Object.entries(this.database).filter(e => e[0] !== "queries"));
      console.log(meta);
   }

   async LoadQueryTemplates() {
      // define table names
      const table_names = ["region", "nation", "customer", "lineitem", "orders", "partsupp", "part", "supplier"];
      // define database postfix
      const table_name_postfix = "_" + this.database.database_id;
      this.query_templates = [];
      for (let query_id = 1; query_id <= 23; query_id++) {
         this.query_templates[query_id] = fs.readFileSync(this.database_config.query_template_path + "/" + query_id + ".sql").toString();
         table_names.forEach(table_name => {
            this.query_templates[query_id] = this.query_templates[query_id].replaceAll(":" + table_name, table_name + table_name_postfix);
         });
      }
      // split query 23 - which is an edge case - into subqueries
      this.query_templates[23] = this.query_templates[23].split(":split:");
   }

   async RunQueryStream() {
      let outstanding = 0;
      this.total_start_delay = 0;
      this.remaining_retries = 100;
      const start_of_run_ts = Date.now() + 2000;

      // Prepare CSV lines
      const lines = [];
      lines.push("query_stream_id,query_id,start,relative_start,query_duration,query_duration_with_queue,start_delay");

      const RunQuery = async (query, idx) => {
         const actual_start_ts = Date.now();
         const planned_start_ts = start_of_run_ts + query.start;
         const start_delay = actual_start_ts - planned_start_ts;


         //TO DO = DAYS IN Q1 NEGATIVE
         // Make sure we do not start too early
         if (start_delay < 0) {
            setTimeout(() => RunQuery(query, idx), -start_delay);
            return;
         }

         // Run query
         const query_template = this.query_templates[query.query_id];
         let res;
         while (this.remaining_retries > 0) {
            try {
               if (query.query_id !== 23) {
                  // log query text with filled binds
                  console.log("[" + idx + "] Running: " + RedshiftPool._FillBinds(query_template, query.arguments).replaceAll("\n", " "));
                  res = (await this.database_connection.RunSync(query_template, query.arguments));
               } else {
                  console.log("[" + idx + "] Running (not really): " + query_template.map(qt => RedshiftPool._FillBinds(qt, query.arguments).replaceAll("\n", " ")));
                  // res.time = 0;
                  // res = (await this.database_connection.RunArraySync(query_template, query.arguments));
               }
            } catch (e) {
               console.log("[" + idx + "] Failed: " + e);
               this.remaining_retries--;
               if (this.remaining_retries === 0) {
                  throw "Retries exceeded";
               }
               continue;
            }
            break;
         }
         var query_duration = 0
         if (query.query_id !== 23) {
            query_duration = res.time;
         }
         // Track time
         const done_ts = Date.now();
         const query_duration_with_queue = done_ts - planned_start_ts;
         this.query_execution_log[idx] = {
            query_id: query.query_id,
            start: actual_start_ts,
            relative_start: actual_start_ts - start_of_run_ts,
            query_duration: query_duration,
            query_duration_with_queue: query_duration_with_queue,
            start_delay: start_delay
         };
         this.total_start_delay += start_delay;
         outstanding--;

         // Append to CSV lines
         lines.push([
         this.database.database_id,
         query.query_id,
         actual_start_ts,
         actual_start_ts - start_of_run_ts,
         query_duration,
         query_duration_with_queue,
         start_delay
         ].join(","));

         console.log("[" + idx + "] Completed query stats: " + query.query_id + ", " + query_duration + ", " + query_duration_with_queue + ", " + (actual_start_ts - start_of_run_ts));
      }
      this.database.queries.forEach((query, idx) => {
         outstanding++;
         setTimeout(() => RunQuery(query, idx), (start_of_run_ts - Date.now()) + query.start);
      });

      // Wait till all are done
      while (outstanding !== 0) {
         await (() => new Promise((r) => setTimeout(r, 100)))();
      }
      const total_time = Date.now() - start_of_run_ts;

      // join into CSV content
      const csv = lines.join("\n") + "\n";

      // 1) print to console
      console.log("-- START CSV --");
      console.log(csv.trim());
      console.log("-- STOP CSV --");
      console.log("total_time: " + total_time);
      console.log("total_lost: " + this.total_start_delay);
      console.log("query_duration: " + this.query_execution_log.reduce((a, b) => a + b.query_duration, 0));
      console.log("query_duration_with_queue: " + this.query_execution_log.reduce((a, b) => a + b.query_duration_with_queue, 0));
      console.log("total_cost: " + this.total_cost);
      console.log("total_scanned: " + this.total_scanned);

      // 2) write out to file
      const outPath = path.resolve(__dirname, 'query_log.csv');
      fs.writeFile(outPath, csv, 'utf8', err => {
         if (err) {
         console.error("Failed to write CSV:", err);
         } else {
         console.log(`Wrote CSV to ${outPath}`);
         }
      });
      // console.log("-- START CSV --");
      // console.log("query_stream_id,query_id, start,relative_start,query_duration,query_duration_with_queue,start_delay");
      // this.query_execution_log.forEach(q => {
      //    console.log([this.database.database_id, q.query_id, q.start, q.relative_start, q.query_duration, q.query_duration_with_queue, q.start_delay].join(","));
      // });
      // console.log("-- STOP CSV --");
      // console.log("total_time: " + total_time);
      // console.log("total_lost: " + this.total_start_delay);
      // console.log("query_duration: " + this.query_execution_log.reduce((a, b) => a + b.query_duration, 0));
      // console.log("query_duration_with_queue: " + this.query_execution_log.reduce((a, b) => a + b.query_duration_with_queue, 0));
      // console.log("total_cost: " + this.total_cost);
      // console.log("total_scanned: " + this.total_scanned);
   }
}

async function main() {
   const query_stream_id = process.argv[2];
   console.log("query_stream_id: " + query_stream_id);

   const executor = new QueryStreamExecuter(RedshiftPool.GetConfig());
   await executor.LoadQueryStream(query_stream_id);
   await executor.LoadQueryTemplates();
   await executor.RunQueryStream();
   console.log(chalk.cyan("\nNormal program exit: done :)"));
}

main();
