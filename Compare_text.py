###File to compare to text files.
import difflib

with open('c:\\Users\\Jeffrey.Magouirk\\Downloads\\202007_Job 2 - NUMERATOR_AFCHIPS4_sharepoint_version.txt', 'r') as file1:
    f1_text = file1.read()


with open('c:\\Users\\Jeffrey.Magouirk\\Downloads\\Job 2 - NUMERATOR_AFCHIPS4_J5_transition.txt', 'r') as file2:
    f2_text = file2.read()    


for line in difflib.unified_diff(f1_text, f2_text, fromfile='file1',
                                 tofile='file2',lineterm=''):
    print(line)

