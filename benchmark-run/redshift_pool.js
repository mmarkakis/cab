import AWS from "aws-sdk";
import pkg from 'pg';

const { Pool } = pkg;

// Define connection options for Redshift
const connection_options = {
   user: process.env.REDSHIFT_DB_USER,
   password: process.env.REDSHIFT_DB_PASSWORD,
   host: process.env.REDSHIFT_ENDPOINT,
   port: process.env.PORT,
   database: process.env.REDSHIFT_DATABASE,
   ssl: process.env.SSL
};

export default class RedshiftPool {
   static GetConfig() {
      return {
         name: 'redshift',
         connector: RedshiftPool,
         query_template_path: 'sql_redshift',
      };
   }

   // Define the constructor function for RedshiftPool, which takes two arguments 
   constructor(max_connection_count, skip_results) {

      // Check that all connection options are defined; if not, log an error message and exit the process
      if (Object.keys(connection_options).some(key => connection_options[key] == null)) {
         console.log('connection_options Redshift: ' + JSON.stringify(connection_options, null, 2));
         console.log('Please define all these options :)');
         process.exit(-1);
      }

      // Initialize the object with connection options, connection counts, and a connection pool
      this.connection_options = connection_options;
      this.used_connection_count = 0;
      this.max_connection_count = max_connection_count;
      this.skip_results = skip_results;

      this.pool = new Pool({
         ...connection_options,
         max: max_connection_count,
      });
   }

   // Define a private method to execute SQL query on a connection
   async _ExecuteOnConnectionSync(connection, sql, binds) {
      const { rows, rowCount: affectedRows } = await connection.query(sql, binds); 
      return { rows, affectedRows };

   }

   // Waits for a free connection, then starts the task and returns with a promise on the results
   async RunSync(sql, binds) {
      this.used_connection_count++;

      // Need two promises here, this one blocks on the result; the one in _ExecuteOnConnectionSync blocks the connection
      return new Promise(async r => {
         const client = await this.pool.connect();
         const start = Date.now();
         // console.log("SQL", sql, "binds", binds);
         const { rows, affectedRows } = await this._ExecuteOnConnectionSync(client, sql, binds);
         const done = Date.now();
         client.release();
         this.used_connection_count--;
         r({ rows, affectedRows, time: done - start });
      });
   }

   async RunPrintSync(sql, binds) {
      const res = await this.RunSync(sql, binds);
      return res;
   }

   // Waits for a free connection, then starts all the tasks and returns with a promise on all the results
   // Note: the same bind is used for each query
   async RunArraySync(sql, binds) {
      this.used_connection_count++;

      // Need two promises here, this one blocks on the result; the one in _ExecuteOnConnectionSync blocks the connection
      return new Promise(async r => {
         const client = await this.pool.connect();
         const start = Date.now();
         const rows = await Promise.all(sql.map(query => this._ExecuteOnConnectionSync(client, query, binds)));
         const done = Date.now();
         client.release();
         this.used_connection_count--;
         r({ rows, time: done - start });
      });
   }

   // wait for all connections to become free
   async Wait() {
      const sleep = () => new Promise((r) => setTimeout(r, 100));
      while (true) {
         if (this.used_connection_count === 0) {
            return;
         }
         await sleep();
      }
   }

   // function to fill bind of queries. In Redshift, $ is used for binding parameters
   static _FillBinds(sql, binds) {
      if (binds == null) {
         return sql;
      }

      let result = sql;
      for (let i = binds.length - 1; i >= 0; i--) {
         if (binds[i] == null) {
            return result;
         }

         if (typeof (binds[i]) === "number") {
            result = result.replaceAll("$" + (i + 1), binds[i]);
         } else if (typeof (binds[i]) === "string") {
            result = result.replaceAll("$" + (i + 1), "'" + binds[i] + "'");
         } else {
            throw new Error("unknown type for binding");
         }
      }
      return result;
   }
}