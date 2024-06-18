# Note

```bash
# 1. gen `tables.json` file
# train 有 data/tables.json， test 有 data/test_data/tables.json ？ dev没有？ 
python preprocess/get_tables.py

# 2. 这里生成了 train.json 文件中的 sql 字段 这里只是给了一个case，教学用 没看懂？？？
python preprocess/parse_sql_one.py

# 3. 完整的code，生成 train.json 和 dev.json 文件中的 sql 字段
python preprocess/parse_raw_json.py
```
