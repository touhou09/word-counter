import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
import PyPDF2

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("PDF Word Count") \
    .getOrCreate()

# 현재 파일의 경로를 기반으로 PDF 파일 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
pdf_path = os.path.join(current_dir, "sample.pdf")

# PDF 파일에서 텍스트 추출 함수
def extract_text_from_pdf(pdf_path):
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
    return text

# PDF 파일에서 텍스트 추출
text = extract_text_from_pdf(pdf_path)

# 텍스트를 RDD로 변환
rdd = spark.sparkContext.parallelize([text])

# 각 라인을 단어로 분리
words = rdd.flatMap(lambda line: line.split())

# 단어를 DataFrame으로 변환
df = words.map(lambda word: (word, )).toDF(["word"])

# 단어 갯수 세기 및 단어 갯수 순으로 정렬
word_counts = df.groupBy("word").count().orderBy(col("count").desc(), col("word"))

# 모든 결과값 출력
word_counts.show(truncate=False, n=word_counts.count())

# 세션 종료
spark.stop()