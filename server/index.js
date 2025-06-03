import express from 'express';
import mysql2 from 'mysql2/promise';
import dotenv from 'dotenv';
import fs from 'fs/promises';
import path from 'path';

// Load environment variables
dotenv.config();

const app = express();
const PORT = process.env.PORT || 5010;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Activity Logger Class
class ActivityLogger {
    constructor() {
        this.logDir = path.join(process.cwd(), 'logs');
        this.logFile = path.join(this.logDir, 'sync-activity.log');
        this.initializeLogDirectory();
    }

    async initializeLogDirectory() {
        try {
            await fs.mkdir(this.logDir, { recursive: true });
        } catch (error) {
            console.error('Failed to create logs directory:', error.message);
        }
    }

    async log(level, action, details = {}) {
        const timestamp = new Date().toISOString();
        const logEntry = {
            timestamp,
            level,
            action,
            details,
            sessionId: this.generateSessionId()
        };

        const logLine = JSON.stringify(logEntry) + '\n';
        
        try {
            await fs.appendFile(this.logFile, logLine);
            console.log(`[${level.toUpperCase()}] ${action}:`, details);
        } catch (error) {
            console.error('Failed to write to log file:', error.message);
        }
    }

    generateSessionId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2);
    }

    async info(action, details) {
        await this.log('INFO', action, details);
    }

    async warn(action, details) {
        await this.log('WARN', action, details);
    }

    async error(action, details) {
        await this.log('ERROR', action, details);
    }

    async success(action, details) {
        await this.log('SUCCESS', action, details);
    }

    async getLogs(limit = 100) {
        try {
            const data = await fs.readFile(this.logFile, 'utf8');
            const lines = data.trim().split('\n').filter(line => line);
            const logs = lines.slice(-limit).map(line => JSON.parse(line));
            return logs.reverse(); // Most recent first
        } catch (error) {
            return [];
        }
    }
}

const logger = new ActivityLogger();

// First Database Connection (Source DB)
const sourceDbConfig = {
    host: process.env.SOURCE_DB_HOST || 'localhost',
    user: process.env.SOURCE_DB_USER || 'root',
    password: process.env.SOURCE_DB_PASSWORD || '',
    database: process.env.SOURCE_DB_NAME || 'modified_erp',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// Second Database Connection (Target DB)
const targetDbConfig = {
    host: process.env.TARGET_DB_HOST || 'localhost',
    user: process.env.TARGET_DB_USER || 'root',
    password: process.env.TARGET_DB_PASSWORD || '',
    database: process.env.TARGET_DB_NAME || 'live_crm',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

const sourcePool = mysql2.createPool(sourceDbConfig);
const targetPool = mysql2.createPool(targetDbConfig);

// Enhanced Test database connections with logging
async function testConnections() {
    try {
        await logger.info('CONNECTION_TEST_START', { 
            source: sourceDbConfig.database, 
            target: targetDbConfig.database 
        });

        // Test source database
        const sourceConnection = await sourcePool.getConnection();
        await logger.success('SOURCE_DB_CONNECTED', { 
            database: sourceDbConfig.database,
            host: sourceDbConfig.host 
        });
        sourceConnection.release();

        // Test target database
        const targetConnection = await targetPool.getConnection();
        await logger.success('TARGET_DB_CONNECTED', { 
            database: targetDbConfig.database,
            host: targetDbConfig.host 
        });
        targetConnection.release();

        await logger.success('CONNECTION_TEST_COMPLETE', { status: 'All connections successful' });
    } catch (error) {
        await logger.error('CONNECTION_TEST_FAILED', { 
            error: error.message,
            stack: error.stack 
        });
    }
}

// Routes
app.get('/', (req, res) => {
    res.json({ message: 'Server is running with dual database connections!' });
});

// Enhanced Health check with logging
app.get('/health', async (req, res) => {
    const status = {
        server: 'OK',
        sourceDatabase: 'Disconnected',
        targetDatabase: 'Disconnected'
    };

    await logger.info('HEALTH_CHECK_START', {});

    try {
        await sourcePool.execute('SELECT 1');
        status.sourceDatabase = 'Connected';
        await logger.success('SOURCE_DB_HEALTH_OK', { database: sourceDbConfig.database });
    } catch (error) {
        await logger.error('SOURCE_DB_HEALTH_FAILED', { 
            database: sourceDbConfig.database,
            error: error.message 
        });
    }

    try {
        await targetPool.execute('SELECT 1');
        status.targetDatabase = 'Connected';
        await logger.success('TARGET_DB_HEALTH_OK', { database: targetDbConfig.database });
    } catch (error) {
        await logger.error('TARGET_DB_HEALTH_FAILED', { 
            database: targetDbConfig.database,
            error: error.message 
        });
    }

    const allConnected = status.sourceDatabase === 'Connected' && status.targetDatabase === 'Connected';
    await logger.info('HEALTH_CHECK_COMPLETE', { status, allConnected });
    
    res.status(allConnected ? 200 : 500).json(status);
});

// Enhanced Database synchronization functions with logging
async function getTablesStructure(pool, dbName) {
    try {
        await logger.info('GET_TABLES_STRUCTURE_START', { database: dbName });
        
        const [tables] = await pool.execute(`
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
        `, [dbName]);
        
        await logger.info('TABLES_FOUND', { 
            database: dbName, 
            tableCount: tables.length,
            tables: tables.map(t => t.TABLE_NAME)
        });
        
        const tablesStructure = {};
        
        for (const table of tables) {
            const tableName = table.TABLE_NAME;
            const [columns] = await pool.execute(`
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    COLUMN_KEY,
                    EXTRA
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            `, [dbName, tableName]);
            
            tablesStructure[tableName] = columns;
            
            await logger.info('TABLE_STRUCTURE_LOADED', {
                database: dbName,
                table: tableName,
                columnCount: columns.length,
                columns: columns.map(c => c.COLUMN_NAME)
            });
        }
        
        await logger.success('GET_TABLES_STRUCTURE_COMPLETE', { 
            database: dbName,
            totalTables: Object.keys(tablesStructure).length
        });
        
        return tablesStructure;
    } catch (error) {
        await logger.error('GET_TABLES_STRUCTURE_FAILED', {
            database: dbName,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

async function syncDatabaseStructure() {
    const sessionId = logger.generateSessionId();
    try {
        await logger.info('SYNC_DATABASE_START', { sessionId });
        
        const sourceStructure = await getTablesStructure(sourcePool, process.env.SOURCE_DB_NAME || 'modified_erp');
        const targetStructure = await getTablesStructure(targetPool, process.env.TARGET_DB_NAME || 'live_crm');
        
        const syncResults = {
            sessionId,
            tablesCreated: [],
            columnsAdded: [],
            errors: [],
            startTime: new Date().toISOString()
        };
        
        await logger.info('SYNC_ANALYSIS_START', {
            sessionId,
            sourceTables: Object.keys(sourceStructure).length,
            targetTables: Object.keys(targetStructure).length
        });
        
        // Check for missing tables and columns
        for (const [tableName, sourceColumns] of Object.entries(sourceStructure)) {
            if (!targetStructure[tableName]) {
                await logger.info('MISSING_TABLE_DETECTED', { 
                    sessionId, 
                    table: tableName,
                    columnCount: sourceColumns.length
                });
                await createTable(tableName, sourceColumns, syncResults, sessionId);
            } else {
                await logger.info('TABLE_EXISTS_CHECKING_COLUMNS', { 
                    sessionId, 
                    table: tableName 
                });
                await syncTableColumns(tableName, sourceColumns, targetStructure[tableName], syncResults, sessionId);
            }
        }
        
        syncResults.endTime = new Date().toISOString();
        syncResults.duration = new Date(syncResults.endTime) - new Date(syncResults.startTime);
        
        await logger.success('SYNC_DATABASE_COMPLETE', {
            sessionId,
            ...syncResults,
            summary: {
                tablesCreated: syncResults.tablesCreated.length,
                columnsAdded: syncResults.columnsAdded.length,
                errors: syncResults.errors.length
            }
        });
        
        return syncResults;
        
    } catch (error) {
        await logger.error('SYNC_DATABASE_FAILED', {
            sessionId,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

function buildColumnDefinition(column) {
    let definition = column.DATA_TYPE.toUpperCase();
    
    // Handle specific data types with length/precision
    if (column.CHARACTER_MAXIMUM_LENGTH && ['VARCHAR', 'CHAR'].includes(definition)) {
        definition += `(${column.CHARACTER_MAXIMUM_LENGTH})`;
    } else if (column.CHARACTER_MAXIMUM_LENGTH && definition === 'TEXT') {
        // For TEXT types, don't add length as it's not always supported
        definition = 'TEXT';
    } else if (column.NUMERIC_PRECISION && ['DECIMAL', 'NUMERIC'].includes(definition)) {
        definition += `(${column.NUMERIC_PRECISION}${column.NUMERIC_SCALE ? ',' + column.NUMERIC_SCALE : ''})`;
    } else if (definition === 'INT') {
        definition += '(11)';
    } else if (definition === 'BIGINT') {
        definition += '(20)';
    } else if (definition === 'SMALLINT') {
        definition += '(6)';
    } else if (definition === 'TINYINT') {
        definition += '(4)';
    }
    
    // Add nullable constraint
    if (column.IS_NULLABLE === 'NO') {
        definition += ' NOT NULL';
    } else {
        definition += ' NULL';
    }
    
    // Handle default values more carefully
    if (column.COLUMN_DEFAULT !== null && column.COLUMN_DEFAULT !== undefined) {
        const defaultValue = column.COLUMN_DEFAULT;
        
        // Handle timestamp/datetime defaults
        if (defaultValue === 'CURRENT_TIMESTAMP' || 
            defaultValue === 'current_timestamp()' ||
            defaultValue === 'CURRENT_TIMESTAMP()') {
            definition += ' DEFAULT CURRENT_TIMESTAMP';
        }
        // Handle date/datetime zero defaults
        else if (defaultValue === '0000-00-00' || 
                 defaultValue === '0000-00-00 00:00:00') {
            // Skip invalid date defaults as they're not supported in newer MySQL/MariaDB
            // definition += ` DEFAULT '${defaultValue}'`;
        }
        // Handle numeric defaults
        else if (!isNaN(defaultValue) && defaultValue !== '') {
            // Check if the default value is valid for the column type
            if (definition.includes('INT') || 
                definition.includes('DECIMAL') || 
                definition.includes('NUMERIC') ||
                definition.includes('FLOAT') ||
                definition.includes('DOUBLE')) {
                definition += ` DEFAULT ${defaultValue}`;
            }
        }
        // Handle string defaults
        else if (typeof defaultValue === 'string' && defaultValue !== '') {
            // Escape single quotes in default values
            const escapedValue = defaultValue.replace(/'/g, "''");
            definition += ` DEFAULT '${escapedValue}'`;
        }
        // Handle NULL defaults explicitly
        else if (defaultValue === null || defaultValue === 'NULL') {
            if (column.IS_NULLABLE === 'YES') {
                definition += ' DEFAULT NULL';
            }
        }
    }
    
    // Add auto increment (must come before PRIMARY KEY)
    if (column.EXTRA && column.EXTRA.includes('auto_increment')) {
        definition += ' AUTO_INCREMENT';
    }
    
    // Add primary key (must come last)
    if (column.COLUMN_KEY === 'PRI') {
        definition += ' PRIMARY KEY';
    }
    
    return definition;
}

// Enhanced createTable function with better error handling
async function createTable(tableName, columns, syncResults, sessionId) {
    let createTableSQL = ''; // Declare outside try block
    try {
        await logger.info('CREATE_TABLE_START', { 
            sessionId, 
            table: tableName,
            columnCount: columns.length
        });
        
        createTableSQL = `CREATE TABLE \`${tableName}\` (`;
        const columnDefinitions = [];
        const primaryKeys = [];
        
        for (const column of columns) {
            try {
                let columnDef = `\`${column.COLUMN_NAME}\` ${buildColumnDefinition(column)}`;
                
                // Handle composite primary keys separately
                if (column.COLUMN_KEY === 'PRI' && !column.EXTRA?.includes('auto_increment')) {
                    // Remove PRIMARY KEY from individual column and collect for composite key
                    columnDef = columnDef.replace(' PRIMARY KEY', '');
                    primaryKeys.push(column.COLUMN_NAME);
                }
                
                columnDefinitions.push(columnDef);
            } catch (colError) {
                await logger.warn('COLUMN_DEFINITION_SKIPPED', {
                    sessionId,
                    table: tableName,
                    column: column.COLUMN_NAME,
                    error: colError.message
                });
                
                // Create a basic column definition as fallback
                let fallbackDef = `\`${column.COLUMN_NAME}\` ${column.DATA_TYPE}`;
                if (column.IS_NULLABLE === 'NO') {
                    fallbackDef += ' NOT NULL';
                }
                columnDefinitions.push(fallbackDef);
            }
        }
        
        createTableSQL += columnDefinitions.join(', ');
        
        // Add composite primary key if exists
        if (primaryKeys.length > 1) {
            createTableSQL += `, PRIMARY KEY (\`${primaryKeys.join('`, `')}\`)`;
        }
        
        createTableSQL += ')';
        
        await logger.info('EXECUTING_CREATE_TABLE', { 
            sessionId, 
            table: tableName,
            sql: createTableSQL
        });
        
        await targetPool.execute(createTableSQL);
        syncResults.tablesCreated.push(tableName);
        
        await logger.success('CREATE_TABLE_SUCCESS', { 
            sessionId, 
            table: tableName,
            columnsCreated: columns.map(c => c.COLUMN_NAME)
        });
        
    } catch (error) {
        await logger.error('CREATE_TABLE_FAILED', {
            sessionId,
            table: tableName,
            error: error.message,
            sql: createTableSQL || 'SQL not generated'
        });
        syncResults.errors.push(`Table ${tableName}: ${error.message}`);
    }
}

// Enhanced syncTableColumns function with better error handling
async function syncTableColumns(tableName, sourceColumns, targetColumns, syncResults, sessionId) {
    try {
        await logger.info('SYNC_TABLE_COLUMNS_START', { 
            sessionId, 
            table: tableName,
            sourceColumns: sourceColumns.length,
            targetColumns: targetColumns.length
        });
        
        const targetColumnNames = targetColumns.map(col => col.COLUMN_NAME);
        const missingColumns = [];
        
        for (const sourceColumn of sourceColumns) {
            if (!targetColumnNames.includes(sourceColumn.COLUMN_NAME)) {
                missingColumns.push(sourceColumn.COLUMN_NAME);
                
                await logger.info('MISSING_COLUMN_DETECTED', {
                    sessionId,
                    table: tableName,
                    column: sourceColumn.COLUMN_NAME,
                    dataType: sourceColumn.DATA_TYPE
                });
                
                try {
                    // Build column definition without PRIMARY KEY for ALTER TABLE
                    let columnDef = buildColumnDefinition(sourceColumn);
                    columnDef = columnDef.replace(' PRIMARY KEY', ''); // Remove PRIMARY KEY from ALTER
                    
                    const alterSQL = `ALTER TABLE \`${tableName}\` ADD COLUMN \`${sourceColumn.COLUMN_NAME}\` ${columnDef}`;
                    
                    await logger.info('EXECUTING_ADD_COLUMN', {
                        sessionId,
                        table: tableName,
                        column: sourceColumn.COLUMN_NAME,
                        sql: alterSQL
                    });
                    
                    await targetPool.execute(alterSQL);
                    syncResults.columnsAdded.push(`${tableName}.${sourceColumn.COLUMN_NAME}`);
                    
                    await logger.success('ADD_COLUMN_SUCCESS', {
                        sessionId,
                        table: tableName,
                        column: sourceColumn.COLUMN_NAME
                    });
                } catch (colError) {
                    await logger.error('ADD_COLUMN_FAILED', {
                        sessionId,
                        table: tableName,
                        column: sourceColumn.COLUMN_NAME,
                        error: colError.message
                    });
                    syncResults.errors.push(`Table ${tableName} column ${sourceColumn.COLUMN_NAME}: ${colError.message}`);
                }
            }
        }
        
        if (missingColumns.length === 0) {
            await logger.info('TABLE_COLUMNS_IN_SYNC', { 
                sessionId, 
                table: tableName 
            });
        } else {
            await logger.success('SYNC_TABLE_COLUMNS_COMPLETE', {
                sessionId,
                table: tableName,
                columnsAdded: missingColumns.filter(col => 
                    !syncResults.errors.some(err => err.includes(col))
                )
            });
        }
        
    } catch (error) {
        await logger.error('SYNC_TABLE_COLUMNS_FAILED', {
            sessionId,
            table: tableName,
            error: error.message
        });
        syncResults.errors.push(`Table ${tableName} columns: ${error.message}`);
    }
}

// Enhanced API endpoint to trigger synchronization
app.post('/sync-database', async (req, res) => {
    try {
        await logger.info('API_SYNC_REQUEST', { 
            requestTime: new Date().toISOString(),
            userAgent: req.get('User-Agent'),
            ip: req.ip
        });
        
        const results = await syncDatabaseStructure();
        
        await logger.success('API_SYNC_RESPONSE_SUCCESS', { 
            sessionId: results.sessionId,
            summary: {
                tablesCreated: results.tablesCreated.length,
                columnsAdded: results.columnsAdded.length,
                errors: results.errors.length
            }
        });
        
        res.json({
            success: true,
            message: 'Database synchronization completed',
            results: results
        });
    } catch (error) {
        await logger.error('API_SYNC_RESPONSE_ERROR', {
            error: error.message,
            stack: error.stack
        });
        
        res.status(500).json({
            success: false,
            message: 'Database synchronization failed',
            error: error.message
        });
    }
});

// Enhanced API endpoint to compare database structures
app.get('/compare-databases', async (req, res) => {
    try {
        await logger.info('API_COMPARE_REQUEST', { 
            requestTime: new Date().toISOString() 
        });
        
        const sourceStructure = await getTablesStructure(sourcePool, process.env.SOURCE_DB_NAME || 'modified_erp');
        const targetStructure = await getTablesStructure(targetPool, process.env.TARGET_DB_NAME || 'live_crm');
        
        const comparison = {
            sourceTables: Object.keys(sourceStructure).length,
            targetTables: Object.keys(targetStructure).length,
            missingTables: [],
            missingColumns: {}
        };
        
        // Find missing tables
        for (const tableName of Object.keys(sourceStructure)) {
            if (!targetStructure[tableName]) {
                comparison.missingTables.push(tableName);
            }
        }
        
        // Find missing columns
        for (const [tableName, sourceColumns] of Object.entries(sourceStructure)) {
            if (targetStructure[tableName]) {
                const targetColumnNames = targetStructure[tableName].map(col => col.COLUMN_NAME);
                const missingCols = sourceColumns
                    .filter(col => !targetColumnNames.includes(col.COLUMN_NAME))
                    .map(col => col.COLUMN_NAME);
                
                if (missingCols.length > 0) {
                    comparison.missingColumns[tableName] = missingCols;
                }
            }
        }
        
        await logger.success('API_COMPARE_RESPONSE_SUCCESS', { 
            comparison,
            missingTablesCount: comparison.missingTables.length,
            tablesWithMissingColumns: Object.keys(comparison.missingColumns).length
        });
        
        res.json(comparison);
        
    } catch (error) {
        await logger.error('API_COMPARE_RESPONSE_ERROR', {
            error: error.message,
            stack: error.stack
        });
        
        res.status(500).json({
            success: false,
            message: 'Database comparison failed',
            error: error.message
        });
    }
});

// New API endpoint to get activity logs
app.get('/logs', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 100;
        const logs = await logger.getLogs(limit);
        
        res.json({
            success: true,
            logs: logs,
            count: logs.length
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Failed to retrieve logs',
            error: error.message
        });
    }
});

// New API endpoint to download log file
app.get('/logs/download', async (req, res) => {
    try {
        const logFilePath = logger.logFile;
        res.download(logFilePath, 'sync-activity.log');
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Failed to download log file',
            error: error.message
        });
    }
});

// Start server
app.listen(PORT, async () => {
    console.log(`Server is running on port ${PORT}`);
    await logger.info('SERVER_START', { 
        port: PORT,
        startTime: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development'
    });
    testConnections();
});

export { sourcePool, targetPool };