import pandas as pd
import matplotlib as plt

pd.set_option('mode.chained_assignment', None)

#Choose states to filter on. Acceptable values are two digit state codes or 'ALL'. If 'ALL' is chosen, results will not be filtered by state#
stateVar = ['OH']

#Choose Sub-Products to filter on. Acceptable values are valid sub-products or 'ALL'. If 'ALL' is chosen, only total results will be returned
subProductVar = ['Medical debt', 'Other debt']

#Choose Companies to filter on. Acceptable values are valid companies or 'ALL'. If 'ALL' is chosen, companies with < 10 complaints will be filtered out
companyVar  = 'ALL'#['Capio Partners, LLC', 'Accelerated Creditors Services, Inc']


def ConsumerComplaint(state, subProduct, company):
    print('Loading data')
    dfConsumerLoad = pd.read_csv(filepath_or_buffer= 'C:\\Users\\nameyer\Downloads\\Consumer_Complaints.CSV', sep= ',', low_memory=False)

    print('Filtering data')
    dropColumns = ['Date received', 'Issue', 'Product', 'Sub-issue', 'Consumer complaint narrative', 'Company public response', 'ZIP code', 'Tags', 'Consumer consent provided?', 'Submitted via', 'Date sent to company', 'Company response to consumer']

    dfConsumer = dfConsumerLoad.drop(dropColumns, axis= 1)

    if state == 'ALL':
        dfState = dfConsumer

    else:
        dfState = dfConsumer[dfConsumer.State.isin(state)]


    if subProduct != 'ALL':
        dfState = dfState.rename(columns = {'Sub-product' : 'SubProduct'})
        dfSubProd = dfState[dfState.SubProduct.isin(subProduct)]

        print('Transforming data')
        dfSubProd['Timely response?'] = dfSubProd['Timely response?'].replace(['Yes', 'No'], ['0', '1']).astype('int16')

        dfSubProd['Consumer disputed?'] = dfSubProd['Consumer disputed?'].fillna('0')
        dfSubProd['Consumer disputed?'] = dfSubProd['Consumer disputed?'].replace(['Yes', 'No'], ['1', '0']).astype('int16')

        print('Aggregating data')
        dfSubProdCount = dfSubProd[['Company','SubProduct', 'Complaint ID']].groupby(['SubProduct','Company']).count().rename(columns={'Complaint ID': 'TotalComplaints'})

        ##Find companies with > 10 total complaints. Will use to filter out companies with <= 10 total complaints
        dfTotalCount = dfState[['Company', 'Complaint ID']].groupby('Company').count().rename(columns={'Complaint ID': 'TotalComplaints'})

        if company == 'ALL':
            dfTotalCount = dfTotalCount[dfTotalCount['TotalComplaints'] > 10]  # Eliminating companies that have less than 10 total complaints

        else:
            dfTotalCount = dfTotalCount.loc[dfTotalCount.index.isin(company)]

        dfSubProdResponse = dfSubProd[['Company','SubProduct', 'Timely response?']].groupby(['SubProduct','Company']).sum()
        dfSubProdDispute = dfSubProd[['Company','SubProduct', 'Consumer disputed?']].groupby(['SubProduct','Company']).sum()

        print('Merging data')
        dfSubProdAgg = dfSubProdCount.merge(dfSubProdResponse, how='inner', left_index=True, right_index=True).merge(dfSubProdDispute, how='inner',left_index=True,right_index=True).merge(dfTotalCount, how= 'inner', left_index= True, right_index= True)

        dfSubProdAgg = dfSubProdAgg.drop('TotalComplaints_y', axis= 1)
        dfSubProdAgg = dfSubProdAgg.rename(columns={'TotalComplaints_x': 'TotalComplaints'})

        dfSubProdAgg['UntimelyPrct'] = dfSubProdAgg['Timely response?'] / dfSubProdAgg['TotalComplaints']
        dfSubProdAgg['DisputedPrct'] = dfSubProdAgg['Consumer disputed?'] / dfSubProdAgg['TotalComplaints']
        dfSubProdAgg['CombinedPrct'] = (dfSubProdAgg['UntimelyPrct'] + dfSubProdAgg['DisputedPrct']) / 2

        dfSubProdFinal = dfSubProdAgg[['TotalComplaints', 'UntimelyPrct', 'DisputedPrct', 'CombinedPrct']].sort_values(by=['CombinedPrct', 'TotalComplaints'])

    print('Transforming data')
    dfState['Timely response?'] = dfState['Timely response?'].replace(['Yes', 'No'], ['0', '1']).astype('int16')

    dfState['Consumer disputed?'] = dfState['Consumer disputed?'].fillna('0')
    dfState['Consumer disputed?'] = dfState['Consumer disputed?'].replace(['Yes', 'No'], ['1', '0']).astype('int16')

    print('Aggregating data')
    dfCount = dfState[['Company','Complaint ID']].groupby('Company').count().rename(columns = {'Complaint ID' : 'TotalComplaints'})

    if company == 'ALL':
        dfCount = dfCount[dfCount['TotalComplaints'] > 10]  #Eliminating companies that have less than 10 total complaints

    else:
        dfCount = dfCount.loc[dfCount.index.isin(company)]

    dfResponse = dfState[['Company','Timely response?']].groupby('Company').sum()
    dfDispute = dfState[['Company','Consumer disputed?']].groupby('Company').sum()

    print('Merging data')
    dfAgg = dfCount.merge(dfResponse, how= 'inner', left_index= True, right_index= True).merge(dfDispute, how= 'inner', left_index= True, right_index= True)

    print('Finding percentages')
    dfAgg['UntimelyPrct'] = dfAgg['Timely response?'] / dfAgg['TotalComplaints']
    dfAgg['DisputedPrct'] = dfAgg['Consumer disputed?'] / dfAgg['TotalComplaints']
    dfAgg['CombinedPrct'] = (dfAgg['UntimelyPrct'] + dfAgg['DisputedPrct']) / 2


    dfCompanyFinal = dfAgg[['TotalComplaints', 'UntimelyPrct', 'DisputedPrct', 'CombinedPrct']].sort_values(by=['CombinedPrct', 'TotalComplaints'])

    return dfSubProdFinal, dfCompanyFinal


dfSubProdFinal, dfCompanyFinal = ConsumerComplaint(stateVar, subProductVar, companyVar)

print(dfSubProdFinal.head(20))
print(dfCompanyFinal.head(20))

spPlot = dfSubProdFinal[['UntimelyPrct', 'DisputedPrct', 'CombinedPrct']].head(20).plot(kind = 'bar')
cPlot = dfCompanyFinal[['UntimelyPrct', 'DisputedPrct', 'CombinedPrct']].head(20).plot(kind = 'bar')

spPlot.plot()
cPlot.plot()

dfSubProdFinal.to_csv('C:\\Users\\nameyer\Downloads\\SubProduct.CSV')
dfCompanyFinal.to_csv('C:\\Users\\nameyer\Downloads\\Company.CSV')
