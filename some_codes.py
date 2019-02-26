# dickey Fuller test
adftest = adfuller(df['column_name'],autolag = 'AIC')
adfoutput = pd.Series(adftest[0:4],index = ['adf','p-value','lags_used','numobs'])

for key,value in dftest[4].items():
	adfoutput['critical_value(%s)'%key] = value

print(adfoutput)

# 
