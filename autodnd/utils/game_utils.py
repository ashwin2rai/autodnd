from random import randrange


def roll_dice(type_of_dice: str = "1d6") -> int:
    type_of_dice_processed = type_of_dice
    if type_of_dice[0] == "d":
        type_of_dice_processed = "1" + type_of_dice
    vals = [int(i) for i in type_of_dice_processed.split("d")]
    return sum([(randrange(vals[1]) + 1) for j in range(vals[0])])
