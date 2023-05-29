print("Hello")

from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "15g") \
        .appName('cub-app') \
        .getOrCreate()

data = [
  ('a', '1',20,10,102),   
  ('a', '1',30,16,4),
  ('a', '1',50,91,5),
  ('a', '1',60,34,2),
  ('a', '1',189,2300,34),
  ('a', '2',33,87,4),
  ('a', '2',86,90,5),
  ('a','2',89,35,2),
  ('a', '2',90,24,44),
  ('a', '2',40,97,3),
  ('a', '2',1,21,3),
  ('b', '1',45,96,4),   
  ('b', '1',56,99,5),
  ('b', '1',89,23,2),
  ('b', '1',98,64,3),
  ('b', '2',86,3200,4),
  ('b', '2',1145,54,3),
  ('b', '2',67,95,2),
  ('b','2',86,70,3),
  ('b', '2',91,64,4),
  ('b', '2',2,53,4),
  ('b', '2',4,87,5)
]
df = (spark.createDataFrame(data, ['cd','segment','price','income','Age']))



input_file="/home/pradeep.k@zucisystems.com/model_workspace/10/JL/Agri/RFC/Cleaning/cleaning.csv"
df = spark.read.csv(input_file,inferSchema =True,header=True)

print("Rows= ",df.count())
print("Columns= ",len(df.columns))
df.show()

from pyspark.sql import functions as f
class Outlier():
    def __init__(self, df):
        self.df = df
    def _calculate_bounds(self):
        bounds = {
            c: dict(
                zip(["q1", "q3"], self.df.approxQuantile(c, [0.25, 0.75], 0))
            )
            for c, d in zip(self.df.columns, self.df.dtypes) if c in ["PRINCIPAL_AMOUNT"]
        }
        for c in bounds:
            iqr = bounds[c]['q3'] - bounds[c]['q1']
            bounds[c]['min'] = bounds[c]['q1'] - (iqr * 1.5)
            bounds[c]['max'] = bounds[c]['q3'] + (iqr * 1.5)
        print(bounds)
        return bounds

    def _flag_outliers_df(self):
        bounds = self._calculate_bounds()
        outliers_col = [
            f.when(
                ~f.col(c).between(bounds[c]['min'], bounds[c]['max']),
                f.col(c)
            ).alias(c + '_outlier')
            for c in bounds]
        
        return self.df.select(*outliers_col)
    
    def show_outliers(self):
        outlier_df = self._flag_outliers_df()
        for outlier in outlier_df.columns:
            a=outlier_df.select(outlier).filter(f.col(outlier).isNotNull()).collect()
            for i in a:
                i=str(i)
                # print(i)
                # print(outlier[:outlier.rfind("_")]+"!="+str(i[i.find("=")+1:i.find(")")]))
                self.df=self.df.where(outlier[:outlier.rfind("_")]+"!="+str(i[i.find("=")+1:i.find(")")]))
        return self.df

obj=Outlier(df)

a=obj.show_outliers()
print("start")
a.show()
# print("Rows= ",a.count())
# print("Columns= ",len(a.columns))