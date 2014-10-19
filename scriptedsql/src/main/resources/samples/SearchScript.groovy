/* 
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2013 ForgeRock. All rights reserved.
 * 
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License").  You may not use this file
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at
 * http://opensource.org/licenses/cddl1.php
 * See the License for the specific language governing permissions and limitations
 * under the License.
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://opensource.org/licenses/cddl1.php.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 * Portions Copyrighted 2013 ConnId.
 */
import groovy.sql.Sql;
import groovy.sql.DataSet;

// Parameters:
// The connector sends the following:
// connection: handler to the SQL connection
// objectClass: a String describing the Object class (__ACCOUNT__ / __GROUP__ / other)
// action: a string describing the action ("SEARCH" here)
// log: a handler to the Log facility
// options: a handler to the OperationOptions Map
// query: a handler to the Query Map
//
// The Query map describes the filter used.
//
// query = [ operation: "CONTAINS", left: attribute, right: "value", not: true/false ]
// query = [ operation: "ENDSWITH", left: attribute, right: "value", not: true/false ]
// query = [ operation: "STARTSWITH", left: attribute, right: "value", not: true/false ]
// query = [ operation: "EQUALS", left: attribute, right: "value", not: true/false ]
// query = [ operation: "GREATERTHAN", left: attribute, right: "value", not: true/false ]
// query = [ operation: "GREATERTHANOREQUAL", left: attribute, right: "value", not: true/false ]
// query = [ operation: "LESSTHAN", left: attribute, right: "value", not: true/false ]
// query = [ operation: "LESSTHANOREQUAL", left: attribute, right: "value", not: true/false ]
// query = null : then we assume we fetch everything
//
// AND and OR filter just embed a left/right couple of queries.
// query = [ operation: "AND", left: query1, right: query2 ]
// query = [ operation: "OR", left: query1, right: query2 ]
//
// Returns: A list of Maps. Each map describing one row.
// !!!! Each Map must contain a '__UID__' and '__NAME__' attribute.
// This is required to build a ConnectorObject.

log.info("Entering "+action+" Script");

def sql = new Sql(connection);
def result = []
def where = "";

if (query != null){
    //Need to handle the __UID__ in queries
    if (query.get("left").equalsIgnoreCase("__UID__") && objectClass.equalsIgnoreCase("__ACCOUNT__")) query.put("left","uid");
    if (query.get("left").equalsIgnoreCase("__UID__") && objectClass.equalsIgnoreCase("__GROUP__")) query.put("left","name");
    if (query.get("left").equalsIgnoreCase("__UID__") && objectClass.equalsIgnoreCase("organization")) query.put("left","name")

    // We can use Groovy template engine to generate our custom SQL queries
    def engine = new groovy.text.SimpleTemplateEngine();

    def whereTemplates = [
        CONTAINS:' WHERE $left ${not ? "NOT " : ""}LIKE "%$right%"',
        ENDSWITH:' WHERE $left ${not ? "NOT " : ""}LIKE "%$right"',
        STARTSWITH:' WHERE $left ${not ? "NOT " : ""}LIKE "$right%"',
        EQUALS:' WHERE $left ${not ? "<>" : "="} "$right"',
        GREATERTHAN:' WHERE $left ${not ? "<=" : ">"} "$right"',
        GREATERTHANOREQUAL:' WHERE $left ${not ? "<" : ">="} "$right"',
        LESSTHAN:' WHERE $left ${not ? ">=" : "<"} "$right"',
        LESSTHANOREQUAL:' WHERE $left ${not ? ">" : "<="} "$right"'
    ]

    def wt = whereTemplates.get(query.get("operation"));
    def binding = [left:query.get("left"),right:query.get("right"),not:query.get("not")];
    def template = engine.createTemplate(wt).make(binding);
    where = template.toString();
    log.ok("Search WHERE clause is: "+ where)
}

switch ( objectClass ) {
    case "__ACCOUNT__":
    sql.eachRow("SELECT * FROM Users" + where, {result.add([__UID__:it.uid, __NAME__:it.uid, uid:it.uid, fullname:it.fullname,firstname:it.firstname,lastname:it.lastname,email:it.email,organization:it.organization])} );
    break

    case "__GROUP__":
    sql.eachRow("SELECT * FROM Groups" + where, {result.add([__UID__:it.name, __NAME__:it.name, gid:it.gid, ,description:it.description])} );
    break

    case "organization":
    sql.eachRow("SELECT * FROM Organizations" + where, {result.add([__UID__:it.name, __NAME__:it.name, description:it.description])} );
    break

    default:
    result;
}

return result;
