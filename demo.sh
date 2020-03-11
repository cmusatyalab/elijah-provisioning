#!/usr/bin/env bash
delim='=============================='
hosts=('de.nephele.findcloudlet.org' 'sg.nephele.findcloudlet.org' 'uk.nephele.findcloudlet.org' 'us-east.nephele.findcloudlet.org' 'us-west.nephele.findcloudlet.org')
src='de.nephele.findcloudlet.org'
dest='us-east.nephele.findcloudlet.org'

snapshot='/root/vmware-demo.zip'
nephele_run_flags='-p 3389,443,22443'
frontail_bin='./frontail-linux'
frontail_flags='-d --ui-hide-topbar --disable-usage-stats'
frontail_src_path='/var/nephele/nephele.log'
frontail_dest_path='/var/nephele/nephele.log'

skip_clean=0
skip_lat=0
skip_bw=0

waitforresponse() {
    while true ; do
        read -e -p "Proceed to next step - $1? (y/N): " key
        if [ "$key" = 'n' -o "$key" = 'N' ]; then exit 0; fi
        if [ "$key" = 'y' -o "$key" = 'Y' ]; then break; fi
    done
    echo $delim
    echo ''
}

if [[ "$skip_clean" -eq 0 ]]; then
    waitforresponse "Cleanup nephele hosts [DESTRUCTIVE]"
    echo $delim
    echo "+++Performing cleanup on nephele hosts..."
    echo $delim
    for i in "${hosts[@]}"; do
        echo "$i:"
        echo "Restarting nephele RPC server..."
        ssh root@"$i" "service nephele restart"
        echo "Killing any running qemu processes..."
        ssh root@"$i" "killall -s KILL qemu-system-x86_64"
        echo "Restarting stream-server..."
        ssh root@"$i" "service stream-server restart"
        if [[ "$skip_bw" -eq 0 ]]; then
            echo "Restarting iperf server..."
            ssh root@"$i" "killall iperf"
            ssh root@"$i" "iperf -s -D > /dev/null 2>&1"
        fi
        echo ""
    done
    echo "Setting up frontail on src/dest appropriately..."
    ssh root@"$src" "killall $frontail_bin"
    ssh root@"$dest" "killall $frontail_bin"
    ssh root@"$src" "$frontail_bin $frontail_flags $frontail_src_path"
    ssh root@"$dest" "$frontail_bin $frontail_flags $frontail_dest_path"
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

waitforresponse "Launch VM on $src"
launch_time="date +%s"
title="horizon-demo"
title="$title""$RANDOM"
echo "+++Launching VM ($title) on $src..."
nephele-client "$src" run "$snapshot" "$title" "$nephele_run_flags"

waitforresponse "Handoff $title to $dest"
handoff_time="date +%s"
duration=handoff_time - launch_time
echo "Time between launch and handoff (seconds): $duration"
echo "+++Performing handoff of $title to $dest..."
nephele-client "$src" handoff "$title" "$dest"
