#!/bin/bash
File="DataSet"
dirs=$(ls -l $File | awk '/^d/ {print $NF}')
for dir in $dirs
do
	echo ===========================================
	echo run data $dir
	g++ -O3 $1 -lpthread -fpic
	./a.out $dir
	
	diff $File/$dir/my_ans.txt $File/$dir/answer.txt >> /dev/null
	if [ $? -eq 0 ]; then
		echo "my answer is same with std answer"
	else
		echo "my answer is not same with std answer"
	fi	
        #rm -rf $File/$dir/my_ans.txt
done
