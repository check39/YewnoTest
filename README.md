# YewnoTest

###### 1. I downloaded blogs from Blog Authorship Corpus and store them in HDFS 
###### 2. Created Spark program in scala so it can run on distributed files in parallel.
###### 3. Created G2Test.scala which takes matrix and similar to ChiTest, it returns statistic and pValue as per logic described in assignment.
###### 4. Created WordPostCountsByAuthor utility which helps merging all results for same word. Due to extra pre-processing (explained below), merge is pretty simple, just appending.
###### 5. Created WordSimilarityWithG2Test.scala which reads blogs from HDFS, parses them and creates termsToDocs which represents concise and efficient data structure to retrieve posts where word occurred at least once. 
###### 5.1. So whenever findSimilarity is called for word x and y, it would lookup words in termsToDocs and find number of posts where words have appeared at least once and find probabilities for: p(x, y), p(¬x; y), p(x; ¬y)  and  p(¬x; ¬y).
###### 5.2. Apology for not handling all exceptions in code in case word is not in corpus, etc... 

#### Additional questions
##### 1. Scalability
###### => Using spark would take benifit of distributed processing to run in parallel. 
###### => Since posts for each author were already organized in one file, created efficient termsToDocs map like term -> (artist, list_of_posts). This extra pre-processing would help avoid extra reduceByKey operation which is expensive shuffling operation.
###### => termsToDocs is calculated once and cached for reuse later. This would make findSimilarity function very fast. We can even cache results in HDFS so it survives restarts.
##### 2. Incremental/Streaming updates
###### => As you can see WordPostCountsByAuthor abstracts merging results for each word. So we can add a new functionality to add new blogs and keep running reduce operation to merge results for same words.
###### => We can provide addNewBlogs() function so user can call anytime new blogs added.
###### => And if blogs are added more frequently then we can create file listener for blogs directory and keep merging new blogs into termsIntoDocs.






