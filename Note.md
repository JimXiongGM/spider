# Note

预处理部分
```bash
# 1. gen `tables.json` file
# train 有 data/tables.json， test 有 data/test_data/tables.json ？ dev没有？ 
python preprocess/get_tables.py

# 2. 这里生成了 train.json 文件中的 sql 字段 这里只是给了一个case，教学用  字段作用没看懂？？？
python preprocess/parse_sql_one.py

# 3. parse_sql_one.py的完整版本，生成 train.json 和 dev.json 文件中的 sql 字段
python preprocess/parse_raw_json.py
```

evaluation_examples 部分

evaluation_examples 文件夹只有readme和txt，没有代码。

```bash
# 只有 EXECUTION ACCURACY 和 EXACT MATCHING ACCURACY
# 论文中说有 Component Matching, Exact Matching, and Execution Accuracy 少了第一个？
python evaluation.py
```

新的eval代码： https://github.com/taoyds/test-suite-sql-eval

-----

| Table | Primary Key | Foreign Key |
| --- | --- | --- |
| author | aid |  |
| cite |  | citing references publication(pid), cited references publication(pid) |
| conference | cid |  |
| domain | did |  |
| domain_author | did | did references domain(did), aid references author(aid) |
| domain_conference | did | did references domain(did), cid references conference(cid) |
| domain_journal | did | did references domain(did), jid references journal(jid) |
| domain_keyword | did | did references domain(did), kid references keyword(kid) |
| domain_publication | did | did references domain(did), pid references publication(pid) |
| journal | jid |  |
| keyword | kid |  |
| organization | oid |  |
| publication | pid | cid references conference(cid), jid references journal(jid) |
| publication_keyword | kid | kid references keyword(kid), pid references publication(pid) |
| writes | aid | aid references author(aid), pid references publication(pid) |

改写

def eval_by_db(db, golden_sqls, pred_sqls)
def parse_to_structured_sql(db, sql):
    if db not in self.db_schema:
        self.db_schema[db] = Schema(get_schema(db))
    else:
        db_schema = self.db_schema[db]

