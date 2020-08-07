import sys
import hashlib

nodeHashList = ["177ccecaec32c54b82d5aaafc18a2dadb753e3b1",
                "208f7f72b198dadd244e61801abe1ec3a4857bc9",
                "33d6357cfaaf0f72991b0ecd8c56da066613c089",
                "abf0fd8db03e5ecb199a9b82929e9db79b909643",
                "c25ddd596aa7c81fa12378fa725f706d54325d12"]

nodeInfoList = ["177ccecaec32c54b82d5aaafc18a2dadb753e3b1 5562 avd4 11124",
                "208f7f72b198dadd244e61801abe1ec3a4857bc9 5556 avd1 11112",
                "33d6357cfaaf0f72991b0ecd8c56da066613c089 5554 avd0 11108",
                "abf0fd8db03e5ecb199a9b82929e9db79b909643 5558 avd2 11116",
                "c25ddd596aa7c81fa12378fa725f706d54325d12 5560 avd3 11120"]

replication = 3


def main():
    itemKey = sys.argv[1]
    keyHash = sha1(itemKey)
    print(itemKey + " -> " + keyHash)
    for idx, val in enumerate(nodeHashList):
        if keyHash <= val:
            for i in range(replication):
                print(nodeInfoList[(idx + i) % len(nodeInfoList)])
            break
        elif val == nodeHashList[len(nodeHashList) - 1]:
            for i in range(replication):
                print(nodeInfoList[i % len(nodeInfoList)])
            break


def sha1(data):
    return hashlib.sha1(data.encode()).hexdigest()


if __name__ == "__main__":
    main()
