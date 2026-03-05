from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, round

# Criando SparkSession
spark = SparkSession.builder \
    .appName("BigData_FoodDelivery") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

caminho_arquivo = "dados/delivery_dados_ficticios.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .csv(caminho_arquivo)

df.createOrReplaceTempView("pedidos")

# Maior faturamento
maior_faturamento = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(valor_pedido),2) AS ticket_medio,
        ROUND(AVG(tempo_entrega_min),2) AS tempo_medio_entrega,
        ROUND(SUM(valor_pedido),2) AS faturamento_total
    FROM pedidos
    GROUP BY cidade
    ORDER BY faturamento_total DESC
    LIMIT 1
""")

# Melhor tempo de entrega
melhor_entrega = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(valor_pedido),2) AS ticket_medio,
        ROUND(AVG(tempo_entrega_min),2) AS tempo_medio_entrega,
        ROUND(SUM(valor_pedido),2) AS faturamento_total
    FROM pedidos
    GROUP BY cidade
    ORDER BY tempo_medio_entrega ASC
    LIMIT 1
""")

resultado_sql = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(valor_pedido),2) AS ticket_medio,
        ROUND(AVG(tempo_entrega_min),2) AS tempo_medio_entrega,
        ROUND(SUM(valor_pedido),2) AS faturamento_total
    FROM pedidos
    GROUP BY cidade
    ORDER BY faturamento_total DESC, tempo_medio_entrega ASC
""")

resultado_top1 = spark.sql("""
   SELECT
    cidade,
    faturamento_total,
    ROW_NUMBER() OVER (ORDER BY faturamento_total DESC) AS posicao
FROM (
    SELECT
        cidade,
        ROUND(SUM(valor_pedido),2) AS faturamento_total
    FROM pedidos
    GROUP BY cidade
) base                          
""")

# CÓDIGO SEM UTILIZAÇÃO DE WINDOWS FUNCTION MAIS EFICIENTE EM PEQUENOS REGISTROS
# resultado_top1 = spark.sql("""
#     SELECT
#     cidade,
#     ROUND(SUM(valor_pedido),2) AS faturamento_total
# FROM pedidos
# GROUP BY cidade
# ORDER BY faturamento_total DESC
# LIMIT 1
# """)

print("Cidade com MAIOR FATURAMENTO")
maior_faturamento.show()

print("Cidade com MELHOR ENTREGA")
melhor_entrega.show()

print("Top 1 de vendas")
resultado_top1.show()

resultado_sql.show()

df_pandas = resultado_sql.toPandas()

import matplotlib.pyplot as plt



plt.figure()
plt.bar(df_pandas["cidade"], df_pandas["faturamento_total"])
plt.title("Faturamento Total por Cidade")
plt.xlabel("Cidade")
plt.ylabel("Faturamento Total")
plt.xticks(rotation=45)
plt.show()

# Encerrando Spark
spark.stop()