# https://github.com/ethereum/go-ethereum/blob/master/core/forkid/forkid_test.go#L48
byzantiumHardForkHeight = 4370000
constantinopleHardForkHeight = 7280000

# https://github.com/ethereum/go-ethereum/blob/master/consensus/ethash/consensus.go#L42
homesteadReward = 5000000000000000000
byzantiumReward = 3000000000000000000
constantinopleBlockReward = int(2e+18)


def get_const_reward(height):
    if height >= constantinopleHardForkHeight:
        return constantinopleBlockReward
    if height >= byzantiumHardForkHeight:
        return byzantiumReward
    return homesteadReward


def get_uncle_reward(height, uncle_height):
    const_reward = get_const_reward(height)
    delta = height - uncle_height
    reward = (const_reward * (8-delta)) >> 3

    return reward

