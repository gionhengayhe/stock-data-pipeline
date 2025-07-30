from flask import Flask, jsonify, request
import duckdb

app = Flask(__name__)

@app.route('/dim_time', methods=['GET'])
def get_dim_time():
    conn = duckdb.connect('/opt/airflow/database/config_dwh/mydb.duckdb')
    query = "SELECT * FROM dim_time"
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    return jsonify(data)

@app.route('/dim_news', methods=['GET'])
def get_dim_news():
    conn = duckdb.connect('/opt/airflow/database/config_dwh/mydb.duckdb')
    query = "SELECT * FROM dim_news"
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    return jsonify(data)

@app.route('/dim_topics', methods=['GET'])
def get_dim_topics():
    conn = duckdb.connect('/opt/airflow/database/config_dwh/mydb.duckdb')
    query = "SELECT * FROM dim_topics"
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    return jsonify(data)

@app.route('/dim_companies', methods=['GET'])
def get_dim_companies():
    conn = duckdb.connect('/opt/airflow/database/config_dwh/mydb.duckdb')
    query = "SELECT * FROM dim_companies"
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    return jsonify(data)

@app.route('/fact_news_companies', methods=['GET'])
def get_fact_news_companies():
    conn = duckdb.connect('/opt/airflow/database/config_dwh/mydb.duckdb')
    query = "SELECT * FROM fact_news_companies"
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    return jsonify(data)

@app.route('/fact_news_topics', methods=['GET'])
def get_fact_news_topics():
    conn = duckdb.connect('/opt/airflow/database/config_dwh/mydb.duckdb')
    query = "SELECT * FROM fact_news_topics"
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    return jsonify(data)

@app.route('/fact_candles', methods=['GET'])
def get_fact_candles():
    time_id = request.args.get("time_id")
    conn = duckdb.connect('/opt/airflow/database/config_dwh/mydb.duckdb')

    if time_id:
        query = f"SELECT * FROM fact_candles WHERE time_id = {int(time_id)}"
    else:
        query = "SELECT * FROM fact_candles"

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
