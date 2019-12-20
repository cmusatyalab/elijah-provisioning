#!/usr/bin/env bash
delim='=============================='
src='us-west.nephele.findcloudlet.org'
dest='de.nephele.findlcloudlet.org' 
hosts=('de.nephele.findcloudlet.org' 'sg.nephele.findcloudlet.org' 'uk.nephele.findcloudlet.org' 'us-east.nephele.findcloudlet.org' 'us-west.nephele.findcloudlet.org')
skip_clean=1
skip_lat=0
skip_bw=0

waitforkey() {
    read -n1 -r -p "Proceed to next step - $1? (y/N):" key
    while [ "$key" != 'y' ]; do
        echo ''
        read -n1 -r -p "Proceed to next step - $1? (y/N):" key
    done
    echo ''
}

if [[ "$skip_clean" -eq 0 ]]; then
    echo $delim
    echo "+++Performing cleanup on nephele hosts..."
    echo $delim
    for i in "${hosts[@]}"; do
        echo "$i:"
        echo "Killing any running nephele processess.."
        ssh root@"$i" "killall nephele"
        echo "Killing any running qemu processess.."
        ssh root@"$i" "killall -s KILL qemu-system-x86_64"
        echo "Clearing instances database table..."
        nephele -r "$i" clear -i -f
        echo "Restarting stream-server..."
        ssh root@"$i" "service stream-server restart"
        echo ""
    done
    echo $delim
fi

if [[ "$skip_lat" -eq 0 ]]; then
    echo $delim
    echo "+++Measuring latency to nephele hosts..."
    echo $delim
    for i in "${hosts[@]}"; do
        echo "$i:"
        ping -c 5 -q "$i" | grep rtt
        echo ""
    done
    echo $delim
fi
if [[ "$skip_bw" -eq 0 ]]; then
    echo "+++Measuring bandwidth to nephele hosts..."
    echo $delim
    for i in "${hosts[@]}"; do
        echo "$i:"
        iperf -t 5 -f M -c "$i" | grep sec
        echo ""
    done
    echo $delim
fi

waitforkey "Launch VM on $src"
title="horizon-demo"
title="$title""$RANDOM"
echo "+++Launching VM ($title) on $src..."
nephele -r "$src" run /root/vmware-demo.zip "$title" -p 3389,443,22443

waitforkey "Handoff $title to $dest"
echo "+++Performing handoff of $title to $dest..."
nephele -r "$src" handoff "$title" "$dest"