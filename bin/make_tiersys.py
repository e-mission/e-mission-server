from emission.core.wrapper.tiersys import TierSys
import arrow

if __name__ == '__main__':
    ts = TierSys(0)
    time = arrow.utcnow().shift(weeks=-1).timestamp
    ts.updateTiers(time)
    print(ts.saveTiers())
