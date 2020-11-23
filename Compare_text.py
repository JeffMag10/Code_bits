###File to compare to text files.


import difflib
import os


sharepoint = 'c:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS FUH 2020_J5_transition\\HEDIS FUH 2020\\FUH\\202007'
QA = 'c:\\Users\\Jeffrey.Magouirk\\Downloads\\HEDIS FUH 2020_J5_transition\\HEDIS FUH 2020\\FUH\\202007\\2020QA'
list_files = os.listdir(sharepoint)
list_filesQA = os.listdir(QA)
print('Transition files = ',list_files)
print('QA files =', list_filesQA)
n1 = len(list_filesQA)
print(n1)
n = 1
for i in range(n,n1):
    c1 = sharepoint + '\\' + list_files[i + 1]
    print(c1)


    c2 = QA + '\\' + list_filesQA[i]
    print(c2)

    with open(c1,'r') as file0:
        f0_text = file0.read()

    with open(c2, 'r') as file0a:
        f0a_text = file0a.read()

    d = difflib.Differ()
    diff = d.compare(f0_text,f0a_text)
    print(list_files[i+1],'=',list_filesQA[i],'\n'.join(diff))
