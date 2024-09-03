# Análise de Dados de Filmes do IMDB com Spark e PySpark
Este repositório contém um projeto de análise de dados de filmes do IMDB utilizando Apache Spark e PySpark. O objetivo é demonstrar o uso de Spark para processar e analisar grandes volumes de dados de filmes.

# Requisitos
Antes de começar, certifique-se de ter o Java e o Apache Spark instalados. Utilize os seguintes comandos para instalar o Java e baixar o Spark:
```sh
# Instalar o Java
!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Baixar a versão mais recente do Spark
!wget -O spark-3.5.2-bin-hadoop3.tgz "https://www.apache.org/dyn/closer.lua/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz?action=download"

# Descompactar o Spark
!tar xf /content/spark-3.5.2-bin-hadoop3.tgz
```
# Configuração do Ambiente
Defina as variáveis de ambiente para o Java e o Spark:
```sh
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.2-bin-hadoop3"
```
Instale a biblioteca 'findspark' para localizar o Spark no sistema:

```sh
!pip install -q findspark
```
Importe e inicialize o Spark:
```sh
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master('local')\
        .appName('sparkcolab')\
        .getOrCreate()
```
# Carregamento e Processamento de Dados
1. Importar os Datasets:
```sh
from google.colab import files
arquivo = files.upload()
```
2. Criar DataFrame:
```sh
df = spark.read.csv('movies.csv', header=True, inferSchema=True)
df.printSchema()
df.show()
```
3. Selecionar e Transformar Colunas:
```sh
df_movie = df.select('title', 'year', 'country', 'director', 'votes')
df_vote = df_movie.withColumn('votos', df_movie['votes'].cast('int')).drop('votes')
df_vote.show()
```
4. Aplicar Filtros e Agregações:
```sh
df_vote.filter(df_vote.votos > 500).show()

from pyspark.sql.functions import max
df_max = df_vote.agg(max('votos').alias('max_votos')).show()

df_max_votos = df_vote.filter(df_vote.votos == 2278845)
df_max_votos.show()
```
5. Contar e Ordenar Países:
```sh
from pyspark.sql.functions import count, col
df_sum = df_vote.groupBy('country').count()
df_sum.orderBy(col('count').desc()).show()
```
6. Filtrar Dados:
```sh
df_vote.select('country', 'title', 'votos').filter(df_vote.country.isin('USA', 'India', 'France')).show()

df_vote.select('country', 'title', 'votos').filter(~df_vote.country.isin('USA', 'India', 'France')).show()
```
7. Utilizar SQL no PySpark:
```sh
df_vote.createOrReplaceGlobalTempView('movies')
spark.sql('SELECT country, count(*) as qtd FROM global_temp.movies GROUP BY country ORDER BY qtd desc').show(truncate=False)
```
8.Carregar e Juntar DataFrames:
```sh
df_ratings = spark.read.csv('ratings.csv', header=True, inferSchema=True, sep=',')
df_ratings.show()

df.join(df_ratings, df.imdb_title_id == df_ratings.imdb_title_id, 'inner')\
        .select(df.country, df.title, df.director, df.year, df_ratings.weighted_average_vote, df_ratings.weighted_average_vote)\
        .show(truncate=False)
```
# Executando o Código
Certifique-se de que o Spark e o Java estejam configurados corretamente e que você tenha os arquivos movies.csv e ratings.csv no mesmo diretório que o código.

Execute o script Python para iniciar a análise de dados.

# Contribuições
Sinta-se à vontade para contribuir com melhorias ou correções enviando um pull request.

# Licença
Este projeto está licenciado sob a MIT License.
