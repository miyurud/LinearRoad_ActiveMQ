output=`ps aux|grep 3342`
set -- $output
pid=$2
kill $pid
