from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("BigData_FoodDelivery") \
    .getOrCreate()

print("Spark funcionando!")


# Caminho do arquivo
caminho_arquivo = "dados/dados_loja_fooddelivery.csv"

# Leitura do CSV (atenção ao separador ;)
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ";") \
    .csv(caminho_arquivo)

# Visualizar dados
df.show(5)

# Estrutura
df.printSchema()

# ------------------------------
# ANÁLISES BIG DATA
# ------------------------------

# 1️⃣ Total de pedidos por cidade
pedidos_cidade = df.groupBy("cidade").agg(
    count("*").alias("total_pedidos"),
    sum("valor_pedido").alias("valor_total"),
    avg("tempo_entrega_min").alias("tempo_medio_entrega")
)

pedidos_cidade.show()

# 2️⃣ Desempenho por restaurante
restaurantes = df.groupBy("restaurante").agg(
    count("*").alias("qtd_pedidos"),
    avg("avaliacao_cliente").alias("avaliacao_media"),
    avg("tempo_entrega_min").alias("tempo_medio")
)

restaurantes.show()

# 3️⃣ Análise via Spark SQL
df.createOrReplaceTempView("pedidos")

resultado_sql = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(valor_pedido),2) AS ticket_medio,
        ROUND(AVG(avaliacao_cliente),2) AS avaliacao_media
    FROM pedidos
    WHERE status = 'Entregue'
    GROUP BY cidade
    ORDER BY total_pedidos DESC
""")

resultado_sql.show()

# Finalizar Spark
spark.stop()
