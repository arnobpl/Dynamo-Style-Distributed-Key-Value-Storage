trap 'pkill -P $$' EXIT
adb -s emulator-5554 logcat -s SimpleDynamoProvider >> logs/avd0_5554_11108_log.txt &
adb -s emulator-5556 logcat -s SimpleDynamoProvider >> logs/avd1_5556_11112_log.txt &
adb -s emulator-5558 logcat -s SimpleDynamoProvider >> logs/avd2_5558_11116_log.txt &
adb -s emulator-5560 logcat -s SimpleDynamoProvider >> logs/avd3_5560_11120_log.txt &
adb -s emulator-5562 logcat -s SimpleDynamoProvider >> logs/avd4_5562_11124_log.txt &
wait
