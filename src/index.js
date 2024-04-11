const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    try {
        const { fields, table } = parseQuery(query);
        const filePath = `${table}.csv`;

        // Read CSV file
        const data = await readCSV(filePath);

        // Check if CSV file is empty
        if (data.length === 0) {
            throw new Error(`CSV file "${filePath}" is empty`);
        }

        // Filter the fields based on the query
        return data.map(row => {
            const filteredRow = {};
            fields.forEach(field => {
                if (!row.hasOwnProperty(field)) {
                    throw new Error(`Field "${field}" not found in CSV file`);
                }
                filteredRow[field] = row[field];
            });
            return filteredRow;
        });
    } catch (error) {
        throw new Error(`Error executing query: ${error.message}`);
    }
}

module.exports = executeSELECTQuery;
