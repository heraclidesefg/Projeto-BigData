from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, round

#===================================================
# 1- Criando SparkSession
#===================================================
spark = SparkSession.builder \
    .appName("BigData_FoodDelivery") \
    .master("local[*]") \
    .getOrCreate()

#===================================================
# 2- Caminho do arquivo
#===================================================

caminho_arquivo = "dados/dados_loja_fooddelivery.csv"

#===================================================
# 3- Leitura do CSV (atenção ao separador ;)
#===================================================

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ";") \
    .csv(caminho_arquivo)

#===================================================
# 4- Visualizar estrutura dos dados
#===================================================
print("\nPrimeiros registros:")   
df.show(5)

print("\n Estrutura do DataFrame")
df.printSchema()

#===================================================
# 4. Quantidade total de registros
#===================================================

total_registros = df.count()
print(f"\nTotal de registros: {total_registros}")

#===================================================
# 5. Análise simples (Big Data na prática)
#===================================================

#===================================================
# Total de vendas por restaurante
#===================================================

print("\nTotal de vendas por restaurante:")
df.groupBy("restaurante") \
  .agg(count("*").alias("QTD_Vendas")) \
  .show()

#===================================================
# Valor médio das vendas
#===================================================

print("\nValor médio das vendas:")
#df.select(avg("valor_pedido", 2).alias("media_vendas")).show()
df.select(round(avg("valor_pedido"), 2).alias("media_vendas")).show()

#===================================================
#  Total de pedidos por cidade
#===================================================
pedidos_cidade = df.groupBy("cidade").agg(
    count("*").alias("total_pedidos"),
    round(sum("valor_pedido"),2).alias("valor_total"),
    round(avg("tempo_entrega_min"),2).alias("tempo_medio_entrega")
)

pedidos_cidade.show()

#===================================================
# Desempenho por restaurante
#===================================================
restaurantes = df.groupBy("restaurante").agg(
    count("*").alias("qtd_pedidos"),
    avg("avaliacao_cliente").alias("avaliacao_media"),
    avg("tempo_entrega_min").alias("tempo_medio")
)

restaurantes.show()

#===================================================    
# Finalizando o spark
#===================================================
spark.stop()

