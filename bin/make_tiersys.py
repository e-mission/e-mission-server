from emission.core.wrapper.tiersys import TierSys
import arrow

if __name__ == '__main__':
    ts = TierSys(3)
    time = arrow.utcnow().shift(weeks=-1).timestamp
    print(ts.updateTiers(time))
    print(ts.saveTiers(time))
