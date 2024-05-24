from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col, to_timestamp, concat, lit, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

src_dir = './source_data/'

src_rat_file = 'ratings_small.csv'
src_rat_dir = src_dir + src_rat_file
src_mov_file = 'movies_metadata.csv'
src_mov_dir = src_dir + src_mov_file
src_lnk_file = 'links_small.csv'
src_lnk_dir = src_dir + src_lnk_file

## read source data
rat_df = spark.read.csv(src_rat_dir, header='true', escape='\"', quote='\"', multiLine='true')
movie_df = spark.read.csv(src_mov_dir, header='true', escape='\"', quote='\"', multiLine='true')
lnk_df = spark.read.csv(src_lnk_dir, header='true', escape='\"', quote='\"', multiLine='true')

## covert timestamp column into readable timestamp and date column in yyyy-mm-dd format
rat_df = rat_df.withColumn('timestamp_trans', to_timestamp(col('timestamp').cast('long')))
rat_df = rat_df.withColumn('date', date_format(col('timestamp_trans').cast('timestamp'), 'yyyy-MM-dd'))

## rename column for joining table afterwards
movie_df = movie_df.withColumnRenamed('imdb_id', 'imdb_id_key')
movie_cols = ['imdb_id_key', 'original_title']
movie_df = movie_df.select(movie_cols)

# In this scenario, assume imdb_id is unique as well, so implement drop duplicates
movie_df = movie_df.drop_duplicates(['imdb_id_key'])

## in the movie table, the imdb_id including 'tt' at the beginning of the imdb_id, so also add tt at the beginnning for joining table after wards  
lnk_df = lnk_df.withColumn('imdb_id_key', concat(lit('tt'), col('imdbId')))

# join lnk table to get the imdb_id and join the movie table to get the movie title
joined_df = rat_df.join(lnk_df, on='movieId', how='left').join(movie_df, on='imdb_id_key', how='left')

# aim to get each user highest rating movie title
# if there are same highest ratings encountered, get the movie tilte by latest timestamp   
window = Window.partitionBy('userId').orderBy(desc('rating'), desc('timestamp_trans'))
res_df = joined_df.withColumn('row_num', row_number().over(window)) \
           .where(col('row_num') == 1) \
           .select('userId', 'original_title', 'rating', 'timestamp_trans')

print(res_df.show())