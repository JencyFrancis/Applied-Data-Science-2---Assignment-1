Overview:
This assignment explores the Top 50 best-selling books on Amazon between 2009 and 2019 using Apache Spark and PySpark. Rather than working with pandas or standard Python data tools, the entire analysis is built as a PySpark data processing pipeline.he work is completed inside a Google Colab notebook and graded through CodeGrade.

Dataset:
The dataset is AmazonBooks.csv, which contains records of the Top 50 bestselling books on Amazon for each year from 2009 to 2019. The columns include book title, author, user rating, number of reviews, price, year, genre, and rank.

The Exercises:
There are four exercises, and they build progressively in complexity.
Exercise 1 is worth 5 marks and looks at the authors who appear most frequently in the bestseller lists. For each of those authors, the analysis finds the number of unique titles they had on the list, their average rating, their total number of reviews, and the highest ranking position they ever achieved. This is essentially a grouped aggregation problem — grouping by author and computing multiple summary statistics at once.

Exercise 2 is also worth 5 marks and shifts focus to genre. For both fiction and non-fiction books, the analysis finds the average and total number of reviews for the top 10, top 25, and top 50 of the bestseller lists, broken down by year. This means filtering the data by rank threshold and genre, then aggregating by year — so the output gives a clear picture of how review volumes compare across genres and list positions over time.

Exercise 3 is worth 10 marks and digs into pricing. For each year, the average price of a fiction book and a non-fiction book is calculated separately for the top 10, top 25, and top 50 of the lists. This builds on the structure of Exercise 2 but adds price as the key metric, making it possible to see whether fiction or non-fiction commands higher prices at different list depths and whether pricing trends shifted across the decade.

Exercise 4 is also worth 10 marks and focuses specifically on free books — those where the price is recorded as zero. For this group, the analysis finds the number of unique titles and unique authors. It then compares the average rating and average number of reviews between free and priced books for each year, which gives an interesting angle on whether free books perform differently in terms of reader engagement and satisfaction.

How to Run It:
Open the notebook in Google Colab, make sure the AmazonBooks.csv file is accessible (either uploaded directly or mounted from Drive), and run all cells from top to bottom. PySpark is available in Colab without any additional installation steps.

Conclusion:
The entire analysis is built as a PySpark pipeline, which directly addresses the requirement to write data processing pipelines that exploit Apache Spark. The exercises also demonstrate understanding of how Spark handles large-scale data through distributed grouping, filtering, and aggregation operations — covering the knowledge and understanding outcome around how Apache Spark can be used to handle and process data in practice.
