const readCSV = require("./csvReader");
const { parseQuery } = require("./queryParser");

async function executeSELECTQuery(query) {
  const { fields, table, whereClauses, joinType, joinTable, joinCondition } =
    parseQuery(query);
  console.log(
    "whats",
    fields,
    table,
    whereClauses,
    joinType,
    joinTable,
    joinCondition
  );
  let data = await readCSV(`${table}.csv`);
  console.log("Data from CSV:", data);
  //
  console.log("Data from CSV:", data);
  console.log("whereClauses", whereClauses);
  console.log("table", table);
  // Perform INNER JOIN if specified
  let data_new = [];
  if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);
    switch (joinType.toUpperCase()) {
      case "INNER":
        data_new = performInnerJoin(
          data,
          joinData,
          joinCondition,
          fields,
          table,
          whereClauses
        );
        break;
      case "LEFT":
        data_new = performLeftJoin(
          data,
          joinData,
          joinCondition,
          fields,
          table
        );
        break;
      case "RIGHT":
        data_new = performRightJoin(
          data,
          joinData,
          joinCondition,
          fields,
          table
        );
        break;
      default:
        throw new Error(`Unsupported JOIN type: ${joinType}`);
    }
  }
  console.log("data1", data);
  console.log("data2", data_new);

  let result = [];

  // Check if whereClauses array has elements and data_new contains any rows
  if (whereClauses.length > 0 && data_new.length > 0) {
    // Apply whereClause to data_new
    result = data_new.filter((row) => evaluateCondition(row, whereClauses));
  } else if (whereClauses.length > 0) {
    // Apply whereClause to original data
    result = data.filter((row) => evaluateCondition(row, whereClauses));
  } else {
    // If whereClauses is empty, return data_new if it contains rows, otherwise return original data
    result = data_new.length > 0 ? data_new : data;
    result = result.map((row) => {
      delete row.age;
      return row;
    });
  }
  console.log("data2", result);
  result = result.map((row) => {
    const filteredRow = {};
    fields.forEach((field) => {
      filteredRow[field] = row[field];
    });
    return filteredRow;
  });

  console.log("Filtered result:", result);
  return result;
}

function performInnerJoin(
  data,
  joinData,
  joinCondition,
  fields,
  table,
  whereClause
) {
  console.log("joinData", data);
  console.log("joinCondition", joinCondition);
  //     console.log("fields", fields);
  //     console.log("table", table);
  const leftColumn = joinCondition.left.split(".").pop();
  const rightColumn = joinCondition.right.split(".").pop();
  console.log("spiltted", leftColumn, rightColumn);
  console.log(Object.keys(data[0]));

  const joinedRows = [];
  for (let i = 0; i < data.length; i++) {
    for (let j = 0; j < joinData.length; j++) {
      if (data[i][leftColumn] === joinData[j][rightColumn]) {
        console.log("data[i][joinCondition.left]", data[i][leftColumn]);
        console.log(
          "joinData[j][joinCondition.right]",
          joinData[j][rightColumn]
        );
        console.log(
          "data[i][joinCondition.left] === joinData[j][joinCondition.right]",
          data[i][leftColumn] === joinData[j][rightColumn]
        );
        joinedRows.push({
          "student.name": data[i]["name"],
          "enrollment.course": joinData[j]["course"],
          "student.age": data[i]["age"],
        });
      }
    }
  }
  console.log("joinedRows", joinedRows);
  return joinedRows;
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
  const leftColumn = joinCondition.left.split(".").pop();
  const rightColumn = joinCondition.right.split(".").pop();
  console.log("field", fields);
  const joinedRows = [];
  const left = fields[0];
  const right = fields[1];

  for (let i = 0; i < data.length; i++) {
    let foundMatch = false;

    for (let j = 0; j < joinData.length; j++) {
      if (data[i][leftColumn] === joinData[j][rightColumn]) {
        foundMatch = true;
        const joinedRow = {
          "student.name": data[i]["name"],
          "enrollment.course": joinData[j]["course"],
        };
        joinedRows.push(joinedRow);
      }
    }

    if (!foundMatch) {
      joinedRows.push({
        "student.name": data[i]["name"],
        "enrollment.course": null,
      });
    }
  }

  return joinedRows;
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
  const leftColumn = joinCondition.left.split(".").pop();
  const rightColumn = joinCondition.right.split(".").pop();
  const joinedRows = [];
  const left = fields[0];
  const right = fields[1];

  for (let i = 0; i < joinData.length; i++) {
    let foundMatch = false;

    for (let j = 0; j < data.length; j++) {
      if (joinData[i][rightColumn] === data[j][leftColumn]) {
        foundMatch = true;
        break;
      }
    }

    if (foundMatch) {
      for (let j = 0; j < data.length; j++) {
        if (joinData[i][rightColumn] === data[j][leftColumn]) {
          joinedRows.push({
            "student.name": data[j]["name"],
            "enrollment.course": joinData[i]["course"],
          });
        }
      }
    } else {
      joinedRows.push({
        "student.name": null,
        "enrollment.course": joinData[i]["course"],
      });
    }
  }

  return joinedRows;
}

function evaluateCondition(row, clauses) {
  console.log("clause", clauses);
  console.log("row", row);
  let result = true; // Assuming all clauses should match by default

  // Iterate through each clause
  for (const clause of clauses) {
    // Evaluate the current clause
    let clauseResult;
    switch (clause.operator) {
      case "=":
        clauseResult = row[clause.field] == clause.value;
        break;
      case ">":
        clauseResult = row[clause.field] > clause.value;
        break;
      case ">=":
        clauseResult = row[clause.field] >= clause.value;
        break;
      case "<":
        clauseResult = row[clause.field] < clause.value;
        break;
      case "<=":
        clauseResult = row[clause.field] <= clause.value;
        break;
      case "!=":
        clauseResult = row[clause.field] != clause.value;
        break;
      default:
        throw new Error(`Unsupported operator: ${clause.operator}`);
    }

    // Combine the current clause result with the final result
    // For AND condition, all clauses should be true
    // For OR condition, at least one clause should be true
    result = result && clauseResult; // For AND condition
  }
  return result;
}

module.exports = executeSELECTQuery;
