#Find Min, Max, Avg execution time for each query executed 5-times.
import re
import csv

mydict={}
with open('C:\\4-node-hdp\\run_all.log') as f:
    data=f.read()
    lkey=[]
    for matches in re.findall("Run DAG.*\d+|[qQ]\d+\w*.sql", data):

        if re.match('[qQ]\d+\w*.sql',matches):
            print('matches contains sql name')
            lkey.append(matches)
            print("lkey={}".format(lkey))
        else:
            print('matches contain run time')
            k=lkey.pop()
            print("lkey={}".format(lkey))
            v=float(re.split(" +",matches)[2])
            print("k = {}, V = {}".format(k,v))
            if k in mydict.keys():
                li=mydict[k]
                print('BEFORE APPEND: li={}, type of li={}'.format(li, type(li)))
                li.append(v)
                print('AFTER APPEND : li={}, type of li={}'.format(li,type(li)))
                mydict[k]=li
            else:
                mydict[k]=[v,]


# print(mydict)

for k,v in mydict.items():
    print("{},{},{},{},{}".format(k,min(v),max(v),sum(v)/len(v),v))

with open('C:\\4-node-hdp\\run_all_results_analysis.output', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    spamwriter.writerow(["|Query|Min|Max|Avg|Seq|"])
    for k, v in mydict.items():
        if len(v) > 1:
            lv=v[1:]
        else:
            lv=v
        # print(lv)
        spamwriter.writerow(["|{}|{}|{}|{}|{}|".format(k, min(lv), max(lv), sum(lv) / len(lv), lv)])



"""
mydict:{
'q2.sql': {"execs":[17.53, 13.06, 16.15], "CACHE_MISS":[2312,23432543,34543543]} 
'q12.sql': [15.27, 16.13], 
'q22.sql': [20.18]}
"""


