DAY=$1
TAG=$2

$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py 079e0f1a-c440-3d7c-b0e7-de160f748e35 $DAY /tmp/iphone.1.$TAG.$DAY
$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py c76a0487-7e5a-3b17-a449-47be666b36f6 $DAY /tmp/iphone.2.$TAG.$DAY
$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py c528bcd2-a88b-3e82-be62-ef4f2396967a $DAY /tmp/iphone.3.$TAG.$DAY
$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py 95e70727-a04e-3e33-b7fe-34ab19194f8b $DAY /tmp/iphone.4.$TAG.$DAY

$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py e471711e-bd14-3dbe-80b6-9c7d92ecc296 $DAY /tmp/android.1.$TAG.$DAY
$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py fd7b4c2e-2c8b-3bfa-94f0-d1e3ecbd5fb7 $DAY /tmp/android.2.$TAG.$DAY
$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py 86842c35-da28-32ed-a90e-2da6663c5c73 $DAY /tmp/android.3.$TAG.$DAY
$PWD/e-mission-py.bash bin/debug/extract_timeline_for_day_and_user.py 3bc0f91f-7660-34a2-b005-5c399598a369 $DAY /tmp/android.4.$TAG.$DAY
