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

# 단어 갯수 세기
word_counts = df.groupBy("word").count()

# 결과를 CSV로 저장
word_counts.write.mode("overwrite").csv("/data/word_counts.csv", header=True)

# 결과를 JSON으로 저장
word_counts.write.mode("overwrite").json("/data/word_counts.json")

# 세션 종료
spark.stop()