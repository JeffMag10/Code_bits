### Useful python, self written functions


#### Create a dataframe with counts and percentages

def percent_table(var_1):
    import pandas as pd
    df_0 = pd.DataFrame(var_1.value_counts().sort_values(ascending=False))
    sum_0 = df_0.iloc[0:,].sum()
    df_0['percentage'] = round((df_0.iloc[0:,]/sum_0)*100,2)
    df_0 = df_0.rename(columns={0:'Counts'})
    print(df_0.iloc[0:40,])

## Create a function to read count characters, words and line in a text file and output to the screen

def count_characters(path,filename):
    pFn = path + '/' + filename
    print(pFn)
    with open(pFn) as infile:
        lines = 0
        words = 0
        characters = 0
        for line in infile:
            wordslist = line.split()
            lines = lines+1
            words = words + len(wordslist)
            characters += sum(len(word) for word in wordslist)
    print('================================')
    print('Number of lines =',lines) 
    print('Number of words =',words)
    print('Number of characters =',characters)
    print('================================')


## Create a function to read in a text file and output to the screen

def read_in_text_file(path,filename):
    pFn = path + '/' + filename
    print('Combined path =', pFn)
    with open(pFn,'r') as file0a:
        f0a_text = file0a.read()
        print(f0a_text)
