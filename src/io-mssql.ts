// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// import { IoSqlite } from '@rljson/io-sqlite';
// import { Json } from '@rljson/json';
import sql from 'mssql';

export class IoMssql {
  // public createConfig(): Json {

  public async connectToDatabase(): Promise<void> {
    try {
      const config = {
        user: 'your_username',
        password: 'your_password',
        server: 'localhost',
        database: 'your_database',
        options: {
          encrypt: true, // Use encryption for data transfer (recommended)
          trustServerCertificate: true, // Required for self-signed certificates
        },
      };

      // Create a test database if it doesn't exist
      const createDbQuery = `
      IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '${config.database}')
      BEGIN
          CREATE DATABASE [${config.database}]
      END
    `;

      // Connect to the server (without specifying a database)
      const serverPool = await sql.connect({
        user: config.user,
        password: config.password,
        server: config.server,
        options: config.options,
      });

      // Execute the query to create the database
      await serverPool.request().query(createDbQuery);
      console.log(`Database '${config.database}' is ready.`);

      // Close the server connection
      await serverPool.close();

      // Create a connection pool
      const pool = await sql.connect(config);
      console.log('Connected to the database');

      // Example query
      const result = await pool.request().query('SELECT * FROM your_table');
      console.log(result);

      // Close the connection
      await pool.close();
    } catch (err) {
      console.error('Database connection failed:', err);
    }
  }
}

// Instantiate and use the IoMssql class
const ioMssql = new IoMssql();
ioMssql.connectToDatabase();
