/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// TODO jvs 15-Nov-2003:  SQL standard allows parentheses in the FROM list for
// building up non-linear join trees (e.g. OUTER JOIN two tables, and then INNER
// JOIN the result).  Also note that aliases on parenthesized FROM expressions
// "hide" all table names inside the parentheses (without aliases, they're
// visible).
//
// We allow CROSS JOIN to have a join condition, even though that is not valid
// SQL; the validator will catch it.

/**
 * Parses the FROM clause for a SELECT.
 *
 * <p>FROM is mandatory in standard SQL, optional in dialects such as MySQL,
 * PostgreSQL. The parser allows SELECT without FROM, but the validator fails
 * if conformance is, say, STRICT_2003.
 */
SqlNode DruidFromClause() :
{
    SqlNode e, e2, condition;
    SqlLiteral natural, joinType, joinConditionType;
    SqlNodeList list;
    SqlParserPos pos;
}
{
    e = TableRef()
    (
        LOOKAHEAD(2)
        (
            // Decide whether to read a JOIN clause or a comma, or to quit having
            // seen a single entry FROM clause like 'FROM emps'. See comments
            // elsewhere regarding <COMMA> lookahead.
            //
            // And LOOKAHEAD(3) is needed here rather than a LOOKAHEAD(2). Because currently JavaCC
            // calculates minimum lookahead count incorrectly for choice that contains zero size
            // child. For instance, with the generated code, "LOOKAHEAD(2, Natural(), JoinType())"
            // returns true immediately if it sees a single "<CROSS>" token. Where we expect
            // the lookahead succeeds after "<CROSS> <APPLY>".
            //
            // For more information about the issue, see https://github.com/javacc/javacc/issues/86
            LOOKAHEAD(3)
            natural = Natural()
            joinType = JoinType()
            e2 = TableRef()
            (
                <ON> {
                    joinConditionType = JoinConditionType.ON.symbol(getPos());
                }
                condition = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                    e = new SqlJoin(joinType.getParserPosition(),
                        e,
                        natural,
                        joinType,
                        e2,
                        joinConditionType,
                        condition);
                }
            |
                <USING> {
                    joinConditionType = JoinConditionType.USING.symbol(getPos());
                }
                list = ParenthesizedSimpleIdentifierList() {
                    e = new SqlJoin(joinType.getParserPosition(),
                        e,
                        natural,
                        joinType,
                        e2,
                        joinConditionType,
                        new SqlNodeList(list.getList(), Span.of(joinConditionType).end(this)));
                }
            |
                {
                    e = new SqlJoin(joinType.getParserPosition(),
                        e,
                        natural,
                        joinType,
                        e2,
                        JoinConditionType.NONE.symbol(joinType.getParserPosition()),
                        null);
                }
            )
        |
            // NOTE jvs 6-Feb-2004:  See comments at top of file for why
            // hint is necessary here.  I had to use this special semantic
            // lookahead form to get JavaCC to shut up, which makes
            // me even more uneasy.
            //LOOKAHEAD({true})
            <COMMA> { joinType = JoinType.COMMA.symbol(getPos()); }
            e2 = TableRef() {
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <CROSS> { joinType = JoinType.CROSS.symbol(getPos()); } <APPLY>
            e2 = TableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <OUTER> { joinType = JoinType.LEFT.symbol(getPos()); } <APPLY>
            e2 = TableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                    SqlLiteral.createBoolean(true, joinType.getParserPosition()));
            }
        )
    )*
    {
        return e;
    }
}
