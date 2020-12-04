###File to search a text file(s) for a word or phrase.


import difflib
import os
import pandas as pd

#path = 'c:\\Users\\Jeffrey.Magouirk\\Downloads\\FLU\\2006'
#path = 'c:\\Users\\Jeffrey.Magouirk\\Downloads\\FLU\\2005'
#path = 'C:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS DIABETES\\2004\\AFCHIPS4\\CHOL'
#path = 'C:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS DIABETES\\2004\\AFCHIPS4\\DoD Metric'
#path = 'C:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS DIABETES\\2004\\AFCHIPS4\\Historic Tables'
#path = 'C:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS DIABETES\\2004\\AFCHIPS4\\LIPIDS'
#path = 'C:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS DIABETES\\2004\\AFCHIPS4\\PreMetric'
#path = 'C:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS DIABETES\\2004\\AFCHIPS4\\PreMH Processing'
path = 'C:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS DIABETES\\2004\\Teradata\\1a PreMetric'



list_files = os.listdir(path)
n1 = len(list_files)
print(n1)
n = 0
c1 = []
for i in range(n,n1):
    c0 = path + '\\' + list_files[i]
    c1.append(c0) 

list_true = []
for i in range(n1):
    filename = c1[i]
    searchfile = open(filename,'r')
    for line in searchfile:
        if "provider_specialty" in line:
            list_0 = list_files[i]
            list_true.append(list_0)
            
print(len(list_true))
print('=======================')
print('List of trues','\n',list_true)
df_0 = pd.DataFrame(list_true,columns=['Filenames'])
df_0 = df_0.drop_duplicates()
df_0.to_csv('list_true.csv')
print('========================')
for j in range(len(list_true)):
    print(list_true[j])
    c2 = path+'\\'+list_true[j]
    with open(c2, 'r') as file0a:
        f0a_text = file0a.read()
        print(f0a_text)

        
