import polars as pl

# Load Gapminder dataset
# Assuming we have a CSV file named 'gapminder.csv'
df = pl.read_csv('gapminder.csv')

# Display the first few rows
print(df.head())

# Filter for the year 2007
df_2007 = df.filter(pl.col("year") == 2007)

# Group by continent and calculate mean life expectancy
mean_life_exp = df_2007.groupby("continent").agg(pl.col("lifeExp").mean().alias("mean_lifeExp"))

# Sort by population in descending order
sorted_df = df_2007.sort("pop", reverse=True)

# Show results
print("Mean life expectancy by continent in 2007:")
print(mean_life_exp)

print("\nCountries sorted by population in 2007:")
print(sorted_df.head())

# Polars supports lazy evaluation, which allows you to chain multiple operations 
# and execute them efficiently at the end. This is particularly useful for large datasets.

# Lazy version of the same operation
df_lazy = df.lazy()

# Chain operations (filter, group by, and aggregate)
lazy_result = (
    df_lazy
    .filter(pl.col("year") == 2007)
    .groupby("continent")
    .agg(pl.col("lifeExp").mean().alias("mean_lifeExp"))
)

# Collect the result (execute the lazy computation)
result = lazy_result.collect()
print(result)