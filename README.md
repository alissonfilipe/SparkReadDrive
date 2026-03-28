# Projeto de Tratamento e Integração de Dados do YouTube (PySpark)

Este documento resume todas as etapas realizadas no projeto de engenharia e análise de dados utilizando **PySpark** e **Google Colab**. O objetivo foi realizar a ingestão, limpeza, transformação, integração e persistência dos dados em formato otimizado.

---

## 1. Leitura de dados a partir do Google Drive

Montamos o Google Drive no Colab e lemos arquivos CSV utilizando PySpark com:

* Cabeçalho (`header=True`)
* Inferência automática de tipos (`inferSchema=True`)

```python
spark.read.option("header", True).option("inferSchema", True).csv(caminho)
```

Arquivos lidos:

* `videos-stats.csv` → `df_video`
* `comments.csv` → `df_comentario`
* `USvideos.csv` → `df_us_videos`

---

## 2. Tratamento de valores nulos

Substituímos valores nulos nas colunas numéricas principais:

```python
df_video = df_video.fillna({
    "Likes": 0,
    "Comments": 0,
    "Views": 0
})
```

Também verificamos a quantidade de nulos em todas as colunas para auditoria de qualidade dos dados.

---

## 3. Remoção de registros inválidos

### Remoção de `Video ID` nulos

```python
df_video = df_video.filter(col("Video ID").isNotNull())
df_comentario = df_comentario.filter(col("Video ID").isNotNull())
```

### Remoção de duplicados

```python
df_video = df_video.dropDuplicates(["Video ID"])
```

---

## 4. Conversão de tipos de dados

Convertendo colunas para inteiros:

```python
df_video = df_video
    .withColumn("Likes", col("Likes").cast("int"))
    .withColumn("Comments", col("Comments").cast("int"))
    .withColumn("Views", col("Views").cast("int"))
```

No dataframe de comentários:

```python
df_comentario = df_comentario
    .withColumn("Likes", col("Likes").cast("int"))
    .withColumn("Sentiment", col("Sentiment").cast("int"))
    .withColumnRenamed("Likes", "Likes Comment")
```

---

## 5. Criação de novas colunas (Feature Engineering)

### Coluna Interaction

Representa o engajamento total do vídeo:

```python
df_video = df_video.withColumn(
    "Interaction",
    col("Likes") + col("Comments") + col("Views")
)
```

### Conversão de datas

```python
df_video = df_video.withColumn(
    "Published At",
    to_date(col("Published At"))
)
```

### Extração do ano

```python
df_video = df_video.withColumn(
    "Year",
    year(col("Published At"))
)
```

---

## 6. Integração de dados (Joins)

### Join entre vídeos e comentários

```python
df_join_video_comments = df_video.join(
    df_comentario,
    on="Video ID",
    how="left"
)
```

### Join entre vídeos e dataset de trending

```python
df_join_video_usvideos = df_video.join(
    df_us_videos,
    on="Title",
    how="left"
)
```

---

## 7. Análise de qualidade dos dados

Contagem de registros:

```python
df_video.count()
df_comentario.count()
```

Contagem de valores nulos por coluna:

```python
df_video.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_video.columns
])
```

---

## 8. Remoção de colunas desnecessárias

Alguns arquivos CSV criaram automaticamente a coluna `_c0`, que foi removida:

```python
df_video = df_video.drop("_c0")
```

---

## 9. Persistência dos dados em formato Parquet

Salvamos os dataframes tratados em formato **Parquet**, que é colunar e otimizado para Big Data.

### Vídeos tratados

```python
df_video.write.mode("overwrite").parquet(
    "/content/drive/MyDrive/videos-tratados-parquet"
)
```

### Vídeos + comentários

```python
df_join_video_comments.write.mode("overwrite").parquet(
    "/content/drive/MyDrive/videos-comments-tratados-parquet"
)
```

---

## 10. Principais conceitos aprendidos

Durante o projeto, foram aplicados conceitos fundamentais de engenharia e análise de dados:

* Ingestão de dados em PySpark
* Limpeza e padronização de dados
* Tratamento de valores nulos
* Conversão de tipos
* Feature engineering
* Joins entre datasets
* Auditoria de qualidade de dados
* Armazenamento em formato otimizado (Parquet)

---

## 11. Possíveis próximos passos

Para evoluir o projeto:

* Criar dashboards com ferramentas como Dash ou Power BI
* Analisar sentimento médio por vídeo
* Calcular métricas como taxa de engajamento
* Treinar modelos de machine learning para prever popularidade de vídeos

---

Este projeto representa um fluxo completo de **ETL (Extract, Transform, Load)**, semelhante ao que é realizado em ambientes profissionais de dados.
