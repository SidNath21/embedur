import pyspark.sql.functions as F
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.types import FloatType

def getNumericalValues(df, features):
    df_numerical = df.select(features).describe().toPandas()
    return df_numerical

def graphNumericalFeatures(df, features, bins = 25, figsize = (20, 16)):
    df.select(features).toPandas().hist(bins = bins, figsize = figsize)

def mergeDataFrames(df1, df2, colToMerge, columns_to_drop = [], withLabel = False):
    if withLabel:
        df = df1.join(df2.withColumn('label', F.lit(1)), colToMerge, 'left').fillna(0).drop(*columns_to_drop)
        return df
    else:
        df = df1.join(df2, colToMerge, 'left').drop(*columns_to_drop)
        return df
    
def getCorrelation(df, features, threshold = 1):
    
    correlated_features = set()

    pdf = df.select(features).toPandas()
    correlation_mat = pdf.corr()
    if len(features) <= 10:
        sns.heatmap(correlation_mat, annot = True)
        plt.show()
    
    for i in range(len(correlation_mat.columns)):
        for j in range(i):
            if(abs(correlation_mat.iloc[i, j]) >= threshold):
                # print(correlation_mat.columns[i], correlation_mat.columns[j], correlation_mat.iloc[i, j])
                colname = correlation_mat.columns[i]
                correlated_features.add(colname)
    
    print()

    corr_pairs = correlation_mat.unstack()
    # print(corr_pairs)
    return correlated_features

def filterDataFrame(df, conditions):
    df_filter = df
    for condition in conditions:
        df_filter = df.filter(condition)
    return df_filter

def filterCol(df, conditions, colName = 'SERIAL_NO'):
    df_filter = filterDataFrame(df, conditions)
    vals = list(df_filter.select(colName).toPandas()[colName])
    return vals

def checkMissingValues(df):
    df_missing = df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
    return df_missing

def applyTransformation(df, func, cols, outputType = FloatType()):
    udf_func = F.udf(lambda x: func(x), outputType)
    for c in cols:
        df = df.withColumn(c, udf_func(c))
    return df

def getColumnCounts(df, colName):
    df_counts = df.groupBy(colName).count().toPandas()
    return df_counts