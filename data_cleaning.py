import csv
import re

init_file = open('/Users/Manos/projects/Foundations_DS/cw2_project/microblogDataset_COMP6235_CW2.csv','rU')
exportFile = open('/Users/Manos/projects/Foundations_DS/cw2_project/cleanedData.csv', 'w+')

Positive_Int = '^?[0-9]+$'
Negative_Int = '^-?[0-9]+$'


file = csv.reader(init_file)
header = next(file)

for cdoc in file:
	if len(cdoc) <= 5:
		continue
	checkIdMember = re.match(Negative_Int,cdoc[1])
	if not checkIdMember is None:
		cdoc[1] = str(abs(long(cdoc[1])))
	output = ','.join(cdoc) + '\n'
	exportFile.write(output)

init_file.close()
exportFile.close()



#import the csv into mongodb
# mongoimport --host=127.0.0.1 -d database -c collection --type csv --file /Users/Manos/projects/Foundations_DS/cw2_project/cleanedData.csv --headerline