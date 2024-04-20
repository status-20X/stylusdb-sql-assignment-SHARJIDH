// src/queryParser.js

function parseQuery(query) {
  query = query.trim();
  // console.log("parse query:",query);
  let selectPart, fromPart;
  const whereSplit = query.split(/\sWHERE\s/i);
  query = whereSplit[0];
  const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;
  console.log("parse where clause:", whereClause);
  const joinSplit = query.split(/\sINNER JOIN\s|\sLEFT JOIN\s|\sRIGHT JOIN\s/i);
  selectPart = joinSplit[0].trim();
  // console.log("joinsplit",joinSplit);
  const joinPart = joinSplit.length > 1 ? joinSplit[1].trim() : null;
  // console.log("join part:",joinPart);
  const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
  const selectMatch = selectPart.match(selectRegex);
  if (!selectMatch) {
    throw new Error("Invalid SELECT format");
  }

  const [, fields, table] = selectMatch;

  let joinTable = null,
    joinCondition = null,
    joinType = null;
  if (query) {
    const {
      joinType: jt,
      joinTable: jtble,
      joinCondition: jcond,
    } = parseJoinClause(query);
    joinType = jt;
    joinTable = jtble;
    joinCondition = jcond;
  }

  let whereClauses = [];
  if (whereClause) {
    whereClauses = parseWhereClause(whereClause);
  }
  console.log("parse where clause", whereClauses);
  return {
    fields: fields.split(",").map((field) => field.trim()),
    table: table.trim(),
    whereClauses,
    joinTable,
    joinCondition,
    joinType,
  };
}

function parseJoinClause(query) {
  // console.log("parseJoinClause", query);
  const joinRegex =
    /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
  // console.log("parseJoinClause", joinRegex);
  const joinMatch = query.match(joinRegex);
  // console.log("parseJoinClause", joinMatch);
  if (joinMatch) {
    return {
      joinType: joinMatch[1].trim(),
      joinTable: joinMatch[2].trim(),
      joinCondition: {
        left: joinMatch[3].trim(),
        right: joinMatch[4].trim(),
      },
    };
  }

  return {
    joinType: null,
    joinTable: null,
    joinCondition: null,
  };
}

function parseWhereClause(whereClause) {
  // Initialize an array to store parsed WHERE clauses
  const whereClauses = [];

  // Split the whereClause by logical operators (AND/OR)
  const conditions = whereClause.split(/\sAND\s|\sOR\s/i);

  // Parse each condition and add it to the whereClauses array
  conditions.forEach((condition) => {
    const conditionRegex = /^(.+?)\s?([=<>!]+)\s?(.+)$/i;
    const conditionMatch = condition.match(conditionRegex);
    if (!conditionMatch) {
      throw new Error("Invalid WHERE condition format");
    }

    const [, field, operator, value] = conditionMatch;
    whereClauses.push({ field, operator, value });
  });

  // Return the parsed WHERE clauses array
  return whereClauses;
}

module.exports = { parseQuery, parseJoinClause };
