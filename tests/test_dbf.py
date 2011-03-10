import unittest
import dbf
import datetime
from decimal import Decimal

# Walker in Leaves -- by Scot Noel -- http://www.scienceandfantasyfiction.com/sciencefiction/Walker-in-Leaves/walker-in-leaves.htm

words = """
Soft rains, given time, have rounded the angles of great towers. Generation after generation, wind borne seeds have brought down cities amid the gentle tangle of their roots. All statues of stone have been worn away.
Still one statue, not of stone, holds its lines against the passing years.
Sunlight, fading autumn light, warms the sculpture as best it can, almost penetrating to its dreaming core. The figure is that of a woman, once the fair sex of a species now untroubled and long unseen. Man sleeps the sleep of extinction. This one statue remains. Behind the grace of its ivory brow and gentle, unseeing eyes, the statue dreams.
A susurrus of voices, a flutter of images, and the dream tumbles down through the long morning. Suspended. Floating on the stream that brings from the heart of time the wandering self. Maya  for that is the statue s name-- is buoyed by the sensation, rising within the cage of consciousness, but asleep. She has been this way for months: the unmoving figure of a woman caught in mid stride across the glade. The warmth of sunlight on her face makes her wonder if she will ever wake again.
Even at the end, there was no proper word for what Maya has become. Robot. Cybernetic Organism. Android. These are as appropriate to her condition as calling the stars campfires of the night sky and equally precise. It is enough to know that her motive energies are no longer sun and sustenance, and though Maya was once a living woman, a scientist, now she inhabits a form of ageless attraction. It is a form whose energies are flagging.
With great determination, Maya moves toward wakefulness. Flex a finger. Move a hand. Think of the lemurs, their tongues reaching out in stroke after stroke for the drip of the honeyed thorns. Though there is little time left to save her charges, Maya s only choice is the patience of the trees. On the day her energies return, it is autumn of the year following the morning her sleep began. Maya opens her eyes. The woman, the frozen machine --that which is both-- moves once more.
Two lemur cubs tumbling near the edge of the glade take notice. One rushes forward to touch Maya s knee and laugh. Maya reaches out with an arthritic hand, cold in its sculpted smoothness, but the lemur darts away. Leaves swirl about its retreat, making a crisp sound. The cub displays a playfulness Maya s fevered mind cannot match. The second cub rolls between her moss covered feet, laughing. The lemurs are her charges, and she is failing them. Still, it is good to be awake.
Sugar maples and sumacs shoulder brilliant robes. In the low sun, their orange and purple hues startle the heart. Of course, Maya has no beating organ, no heart. Her life energies are transmitted from deep underground. Nor are the cubs truly lemurs, nor the sugar maples the trees of old. The names have carried for ten million seasons, but the species have changed. Once the lemurs inhabited an island off the southeast coast of a place called Africa. Now they are here, much changed, in the great forests of the northern climes.
The young lemurs seem hale, and it speaks well for their consanguine fellows. But their true fate lies in the story of DNA, of a few strands in the matriarchal line, of a sequence code-named "hope." No doubt a once clever acronym, today Maya s clouded mind holds nothing of the ancient codes. She knows only that a poet once spoke of hope as "the thing with feathers that perches in the soul." Emily Dickinson. A strange name, and so unlike the agnomen of the lemurs. What has become of Giver-of-Corn?
Having no reason to alarm the cubs, Maya moves with her hands high, so that any movement will be down as leaves fall. Though anxious about Giver-of-Corn, she ambles on to finish the mission begun six months earlier. Ahead, the shadow of a mound rises up beneath a great oak. A door awaits. Somewhere below the forest, the engine that gives her life weakens. Held in sway to its faltering beat her mind and body froze, sending her into an abyss of dreams. She has been striding toward that door for half a year, unknowing if she would ever wake again.
Vines lose their toughened grip as the door responds to Maya s approach. Regretfully, a tree root snaps, but the door shudders to a halt before its whine of power can cross the glade. Suddenly, an opening has been made into the earth, and Maya steps lightly on its downward slope. Without breathing, she catches a scent of mold and of deep, uncirculated water. A flutter like the sound of wings echoes from the hollow. Her vision adjusts as she descends. In spots, lights attempt to greet her, but it is a basement she enters, flickering and ancient, where the footfalls of millipedes wear tracks in grime older than the forest above. After a long descent, she steps into water.
How long ago was it that the floor was dry? The exactitude of such time, vast time, escapes her.
Once this place sustained great scholars, scientists. Now sightless fish skip through broken walls, retreating as Maya wades their private corridors, finding with each step that she remembers the labyrinthine path to the heart of power. A heart that flutters like dark wings. And with it, she flutters too. The closer she comes to the vault in which the great engine is housed, the less hopeful she becomes.
The vault housing the engine rests beneath a silvered arch. Its mirrored surface denies age, even as a new generation of snails rise up out of the dark pool, mounting first the dais of pearled stone left by their ancestors, the discarded shells of millions, then higher to where the overhang drips, layered in egg sacs bright as coral.
Maya has no need to set the vault door in motion, to break the dance of the snails. The state of things tells her all she needs to know. There shall be no repairs, no rescue; the engine will die, and she with it. Still, it is impossible not to check. At her touch, a breath of firefly lights coalesces within the patient dampness of the room. They confirm. The heart is simply too tired to go on. Its last reserves wield processes of great weight and care, banking the fires of its blood, dimming the furnace into safe resolve. Perhaps a month or two in cooling, then the last fire kindled by man shall die.
For the figure standing knee deep in water the issues are more immediate. The powers that allow her to live will be the first to fade. It is amazing, even now, that she remains cognizant.
For a moment, Maya stands transfixed by her own reflection. The silvered arch holds it as moonlight does a ghost. She is a sculpted thing with shoulders of white marble. Lips of stone. A child s face. No, the grace of a woman resides in the features, as though eternity can neither deny the sage nor touch the youth. Demeter. The Earth Mother.
Maya smiles at the Greek metaphor. She has never before thought of herself as divine, nor monumental. When the energies of the base are withdrawn entirely, she will become immobile. Once a goddess, then a statue to be worn away by endless time, the crumbling remnant of something the self has ceased to be. Maya trembles at the thought. The last conscious reserve of man will soon fade forever from the halls of time.
As if hewn of irresolute marble, Maya begins to shake; were she still human there would be sobs; there would be tears to moisten her grief and add to the dark waters at her feet.
In time, Maya breaks the spell. She sets aside her grief to work cold fingers over the dim firefly controls, giving what priorities remain to her survival. In response, the great engine promises little, but does what it can.
While life remains, Maya determines to learn what she can of the lemurs, of their progress, and the fate of the matriarchal line. There will be time enough for dreams. Dreams. The one that tumbled down through the long morning comes to her and she pauses to consider it. There was a big table. Indistinct voices gathered around it, but the energy of a family gathering filled the space. The warmth of the room curled about her, perfumed by the smell of cooking. An ancient memory, from a time before the shedding of the flesh. Outside, children laughed. A hand took hers in its own, bringing her to a table filled with colorful dishes and surrounded by relatives and friends. Thanksgiving?
They re calling me home, Maya thinks. If indeed her ancestors could reach across time and into a form not of the flesh, perhaps that was the meaning of the dream. I am the last human consciousness, and I am being called home.
With a flutter, Maya is outside, and the trees all but bare of leaves. Something has happened. Weeks have passed and she struggles to take in her situation. This time she has neither dreamed nor stood immobile, but she has been active without memory.
Her arms cradle a lemur, sheltering the pubescent female against the wind. They sit atop a ridge that separates the valley from the forest to the west, and Walker-in-Leaves has been gone too long. That much Maya remembers. The female lemur sighs. It is a rumbling, mournful noise, and she buries her head tighter against Maya. This is Giver-of-Corn, and Walker is her love.
With her free hand, Maya works at a stiff knitting of pine boughs, the blanket which covers their legs. She pulls it up to better shelter Giver-of-Corn. Beside them, on a shell of bark, a sliver of fish has gone bad from inattention.
They wait through the long afternoon, but Walker does not return. When it is warmest and Giver sleeps, Maya rises in stages, gently separating herself from the lemur. She covers her charge well. Soon it will snow.
There are few memories after reaching the vault, only flashes, and that she has been active in a semi-consciousness state frightens Maya. She stumbles away, shaking, but there is no comfort to seek. She does not know if her diminished abilities endanger the lemurs, and considers locking herself beneath the earth. But the sun is warm, and for the moment every thought is a cloudless sky. Memories descend from the past like a lost tribe wandering for home.
To the east lie once powerful lands and remembered sepulchers. The life of the gods, the pulse of kings, it has all vanished and gone. Maya thinks back to the days of man. There was no disaster at the end. Just time. Civilization did not fail, it succumbed to endless seasons. Each vast stretch of years drawn on by the next saw the conquest of earth and stars, then went on, unheeding, until man dwindled and his monuments frayed.
To the west rise groves of oaks and grassland plains, beyond them, mountains that shrugged off civilization more easily than the rest.
Where is the voyager in those leaves?
A flash of time and Maya finds herself deep in the forests to the west. A lemur call escapes her throat, and suddenly she realizes she is searching for Walker-in-Leaves. The season is the same. Though the air is crisp, the trees are not yet unburdened of their color.
"Walker!" she calls out. "Your love is dying. She mourns your absence."
At the crest of a rise, Maya finds another like herself, but one long devoid of life. This sculpted form startles her at first. It has been almost wholly absorbed into the trunk of a great tree. The knee and calf of one leg escape the surrounding wood, as does a shoulder, the curve of a breast, a mournful face. A single hand reaches out from the tree toward the valley below.
In the distance, Maya sees the remnants of a fallen orbiter. Its power nacelle lies buried deep beneath the river that cushioned its fall. Earth and water, which once heaved at the impact, have worn down impenetrable metals and grown a forest over forgotten technologies.
Had the watcher in the tree come to see the fall, or to stand vigil over the corpse? Maya knows only that she must go on before the hills and the trees conspire to bury her. She moves on, continuing to call for Walker-in-Leaves.
In the night, a coyote finally answers Maya, its frenetic howls awakening responses from many cousins, hunting packs holding court up and down the valley.
Giver-of-Corn holds the spark of her generation. It is not much. A gene here and there, a deep manipulation of the flesh. The consciousness that was man is not easy to engender. Far easier to make an eye than a mind to see. Along a path of endless complication, today Giver-of-Corn mourns the absence of her mate. That Giver may die of such stubborn love before passing on her genes forces Maya deeper into the forest, using the last of her strength to call endlessly into the night.
Maya is dreaming. It s Thanksgiving, but the table is cold. The chairs are empty, and no one answers her call. As she walks from room to room, the lights dim and it begins to rain within the once familiar walls.
When Maya opens her eyes, it is to see Giver-of-Corm sleeping beneath a blanket of pine boughs, the young lemur s bushy tail twitching to the rhythm of sorrowful dreams. Maya is awake once more, but unaware of how much time has passed, or why she decided to return. Her most frightening thought is that she may already have found Walker-in-Leaves, or what the coyotes left behind.
Up from the valley, two older lemurs walk arm in arm, supporting one another along the rise. They bring with them a twig basket and a pouch made of hide. The former holds squash, its hollowed interior brimming with water, the latter a corn mash favored by the tribe. They are not without skills, these lemurs. Nor is language unknown to them. They have known Maya forever and treat her, not as a god, but as a force of nature.
With a few brief howls, by clicks, chatters, and the sweeping gestures of their tails, the lemurs make clear their plea. Their words all but rhyme. Giver-of-Corn will not eat for them. Will she eat for Maya?
Thus has the mission to found a new race come down to this: with her last strength, Maya shall spoon feed a grieving female. The thought strikes her as both funny and sad, while beyond her thoughts, the lemurs continue to chatter.
Scouts have been sent, the elders assure Maya, brave sires skilled in tracking. They hope to find Walker before the winter snows. Their voices stir Giver, and she howls in petty anguish at her benefactors, then disappears beneath the blanket. The elders bow their heads and turn to go, oblivious of Maya s failures.
Days pass upon the ridge in a thickness of clouds. Growing. Advancing. Dimmed by the mountainous billows above, the sun gives way to snow, and Maya watches Giver focus ever more intently on the line to the west. As the lemur s strength fails, her determination to await Walker s return seems to grow stronger still.
Walker-in-Leaves holds a spark of his own. He alone ventures west after the harvest. He has done it before, always returning with a colored stone, a bit of metal, or a flower never before seen by the tribe. It is as if some mad vision compels him, for the journey s end brings a collection of smooth and colored booty to be arranged in a crescent beneath a small monolith Walker himself toiled to raise. Large stones and small, the lemur has broken two fingers of its left hand doing this. To Maya, it seems the ambition of butterflies and falling leaves, of no consequence beyond a motion in the sun. The only importance now is to keep the genes within Giver alive.
Long ago, an ambition rose among the last generation of men, of what had once been men: to cultivate a new consciousness upon the Earth. Maya neither led nor knew the masters of the effort, but she was there when the first prosimians arrived, fresh from their land of orchids and baobabs. Men gathered lemurs and said to them "we shall make you men." Long years followed in the work of the genes, gentling the generations forward. Yet with each passing season, the cultivators grew fewer and their skills less true. So while the men died of age, or boredom, or despair, the lemurs prospered in their youth.
To warm the starving lemur, Maya builds a fire. For this feat the tribe has little skill, nor do they know zero, nor that a lever can move the world. She holds Giver close and pulls the rough blanket of boughs about them both.
All this time, Maya s thoughts remain clear, and the giving of comfort comforts her as well.
The snow begins to cover the monument Walker-in-Leaves has built upon the ridge. As Maya stares on and on into the fire, watching it absorb the snow, watching the snow conquer the cold stones and the grasses already bowed under a cloak of white, she drifts into a flutter of reverie, a weakening of consciousness. The gate to the end is closing, and she shall never know  never know.
"I ll take it easy like, an  stay around de house this winter," her father said. "There s carpenter work for me to do."
Other voices joined in around a table upon which a vast meal had been set. Thanksgiving. At the call of their names, the children rushed in from outside, their laughter quick as sunlight, their jackets smelling of autumn and leaves. Her mother made them wash and bow their heads in prayer. Those already seated joined in.
Grandmother passed the potatoes and called Maya her little kolache, rattling on in a series of endearments and concerns Maya s ear could not follow. Her mother passed on the sense of it and reminded Maya of the Czech for Thank you, Grandma.
It s good to be home, she thinks at first, then: where is the walker in those leaves?
A hand on which two fingers lay curled by the power of an old wound touches Maya. It shakes her, then gently moves her arms so that its owner can pull back the warm pine boughs hiding Giver-of Corn. Eyes first, then smile to tail, Giver opens herself to the returning wanderer. Walker-in-Leaves has returned, and the silence of their embrace brings the whole of the ridge alive in a glitter of sun-bright snow. Maya too comes awake, though this time neither word nor movement prevails entirely upon the fog of sleep.
When the answering howls come to the ridge, those who follow help Maya to stand. She follows them back to the shelter of the valley, and though she stumbles, there is satisfaction in the hurried gait, in the growing pace of the many as they gather to celebrate the return of the one. Songs of rejoicing join the undisciplined and cacophonous barks of youth. Food is brought, from the deep stores, from the caves and their recesses. Someone heats fish over coals they have kept sheltered and going for months. The thought of this ingenuity heartens Maya.
A delicacy of honeyed thorns is offered with great ceremony to Giver-of-Corn, and she tastes at last something beyond the bitterness of loss.
Though Walker-in-Leaves hesitates to leave the side of his love, the others demand stories, persuading him to the center where he begins a cacophonous song of his own.
Maya hopes to see what stones Walker has brought from the west this time, but though she tries to speak, the sounds are forgotten. The engine fades. The last flicker of man s fire is done, and with it the effort of her desires overcome her. She is gone.
Around a table suited for the Queen of queens, a thousand and a thousand sit. Mother to daughter, side-by-side, generation after generation of lemurs share in the feast. Maya is there, hearing the excited voices and the stern warnings to prayer. To her left and her right, each daughter speaks freely. Then the rhythms change, rising along one side to the cadence of Shakespeare and falling along the other to howls the forest first knew.
Unable to contain herself, Maya rises. She pushes on toward the head of a table she cannot see, beginning at last to run. What is the height her charges have reached? How far have they advanced? Lemur faces turn to laugh, their wide eyes joyous and amused. As the generations pass, she sees herself reflected in spectacles, hears the jangle of bracelets and burnished metal, watches matrons laugh behind scarves of silk. Then at last, someone with sculpted hands directs her outside, where the other children are at play in the leaves, now and forever.
THE END""".split()
numbers = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,199,211,223,227,229,233,239,241,251,257,263,269,271,277,281,283,293,307,311,313,317,331,337,347,349,353,359,367,373,379,383,389,397,401,409,419,421,431,433,439,443,449,457,461,463,467,479,487,491,499,503,509,521,523,541]
floats = []
last = 1
for number in numbers:
    floats.append(float(number ** 2 / last))
    last = number

if dbf.version != (0, 88, 19):
    raise ValueError("Wrong version of dbf -- should be %d.%02d.%03d" % dbf.version)
else:
    print "\nTesting dbf version %d.%d.%d\n" % dbf.version

def permutate(Xs, N):
    if N <= 0:
        yield []
        return
    for x in Xs:
        for sub in permutate(Xs, N-1):
            result = [x]+sub                    # don't allow duplicates
            for item in result:
                if result.count(item) > 1:
                    break
            else:
                yield result
def combinate(Xs, N):
    """Generate combinations of N items from list Xs"""
    if N == 0:
        yield []
        return
    for i in xrange(len(Xs)-N+1):
        for r in combinate(Xs[i+1:], N-1):
            yield [Xs[i]] + r
def index(sequence):
    "returns integers 0 - len(sequence)"
    for i in xrange(len(sequence)):
        yield i    
class Test_Date(unittest.TestCase):
    "Testing Date"
    def test01(yo):
        "Date creation"
        date0 = dbf.Date()
        date1 = dbf.Date()
        date2 = dbf.Date.fromymd('        ')
        date5 = dbf.Date.fromordinal(0)
        date6 = dbf.Date.today()
        date7 = dbf.Date.max
        date8 = dbf.Date.min
        yo.assertRaises(ValueError, dbf.Date.fromymd, '00000')
        yo.assertRaises(ValueError, dbf.Date.fromymd, '00000000')
        yo.assertRaises(ValueError, dbf.Date, 0, 0, 0)
    def test02(yo):
        "Date comparisons"
        nodate1 = dbf.Date()
        nodate2 = dbf.Date()
        date1 = dbf.Date.fromordinal(1000)
        date2 = dbf.Date.fromordinal(2000)
        date3 = dbf.Date.fromordinal(3000)
        yo.compareTimes(nodate1, nodate2, date1, date2, date3)

    def test03(yo):
        "DateTime creation"
        datetime0 = dbf.DateTime()
        datetime1 = dbf.DateTime()
        #datetime2 = dbf.DateTime.fromymd('        ')
        datetime5 = dbf.DateTime.fromordinal(0)
        datetime6 = dbf.DateTime.today()
        datetime7 = dbf.DateTime.max
        datetime8 = dbf.DateTime.min
        #yo.assertRaises(ValueError, dbf.Date.fromymd, '00000')
        #yo.assertRaises(ValueError, dbf.Date.fromymd, '00000000')
        #yo.assertRaises(ValueError, dbf.Date, 0, 0, 0)
    def test04(yo):
        "DateTime comparisons"
        nodatetime1 = dbf.DateTime()
        nodatetime2 = dbf.DateTime()
        datetime1 = dbf.Date.fromordinal(1000)
        datetime2 = dbf.Date.fromordinal(20000)
        datetime3 = dbf.Date.fromordinal(300000)
        yo.compareTimes(nodatetime1, nodatetime2, datetime1, datetime2, datetime3)

    def test05(yo):
        "Time creation"
        time0 = dbf.Time()
        time1 = dbf.Time()
        #time2 = dbf.Date.fromymd('        ')
        #time5 = dbf.Date.fromordinal(0)
        #time6 = dbf.Date.today()
        time7 = dbf.Time.max
        time8 = dbf.Time.min
        #yo.assertRaises(ValueError, dbf.Date.fromymd, '00000')
        #yo.assertRaises(ValueError, dbf.Date.fromymd, '00000000')
        #yo.assertRaises(ValueError, dbf.Date, 0, 0, 0)
    def test06(yo):
        "Time comparisons"
        notime1 = dbf.Time()
        notime2 = dbf.Time()
        time1 = dbf.Date.fromordinal(1000)
        time2 = dbf.Date.fromordinal(2000)
        time3 = dbf.Date.fromordinal(3000)
        yo.compareTimes(notime1, notime2, time1, time2, time3)
    def test07(yo):
        "Date, DateTime, & Time Arithmetic"
        one_day = datetime.timedelta(1)
        today = dbf.Date.today()
        today + one_day
        today - one_day
        #yo.assertRaises(TypeError, today - dbf.Date.fromymd('        '))

    def compareTimes(yo, empty1, empty2, uno, dos, tres):
        yo.assertEqual(empty1, empty2)
        yo.assertEqual(uno < dos, True)
        yo.assertEqual(uno <= dos, True)
        yo.assertEqual(dos <= dos, True)
        yo.assertEqual(dos <= tres, True)
        yo.assertEqual(dos < tres, True)
        yo.assertEqual(tres <= tres, True)
        yo.assertEqual(uno == uno, True)
        yo.assertEqual(dos == dos, True)
        yo.assertEqual(tres == tres, True)
        yo.assertEqual(uno != dos, True)
        yo.assertEqual(dos != tres, True)
        yo.assertEqual(tres != uno, True)
        yo.assertEqual(tres >= tres, True)
        yo.assertEqual(tres > dos, True)
        yo.assertEqual(dos >= dos, True)
        yo.assertEqual(dos >= uno, True)
        yo.assertEqual(dos > uno, True)
        yo.assertEqual(uno >= uno, True)
        yo.assertEqual(uno >= dos, False)
        yo.assertEqual(uno >= tres, False)
        yo.assertEqual(dos >= tres, False)
        yo.assertEqual(tres <= dos, False)
        yo.assertEqual(tres <= uno, False)
        yo.assertEqual(tres < tres, False)
        yo.assertEqual(tres < dos, False)
        yo.assertEqual(tres < uno, False)
        yo.assertEqual(dos < dos, False)
        yo.assertEqual(dos < uno, False)
        yo.assertEqual(uno < uno, False)
        yo.assertEqual(uno == dos, False)
        yo.assertEqual(uno == tres, False)
        yo.assertEqual(dos == uno, False)
        yo.assertEqual(dos == tres, False)
        yo.assertEqual(tres == uno, False)
        yo.assertEqual(tres == dos, False)
        yo.assertEqual(uno != uno, False)
        yo.assertEqual(dos != dos, False)
        yo.assertEqual(tres != tres, False)
class Test_Dbf_Functions(unittest.TestCase):
    "Testing table creation..."
    def test01(yo):
        "dbf tables in memory"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = dbf.Table(':memory:', fieldlist, dbf_type='db3')
                actualFields = table.structure()
                table.close()
                yo.assertEqual(fieldlist, actualFields)
    def test02(yo):
        "dbf table on disk"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = dbf.Table('temptable', ';'.join(fieldlist), dbf_type='db3')
                table.close()
                table = dbf.Table('temptable', dbf_type='db3')
                actualFields = table.structure()
                table.close()
                yo.assertEqual(fieldlist, actualFields)
                last_byte = open(table.filename, 'rb').read()[-1]
                yo.assertEqual(last_byte, '\x1a')
    def test03(yo):
        "fp tables in memory"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'litres F(11,5)', 'blob G', 'graphic P']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = dbf.Table(':memory:', ';'.join(fieldlist), dbf_type='vfp')
                actualFields = table.structure()
                table.close()
                yo.assertEqual(fieldlist, actualFields)
    def test04(yo):
        "fp tables on disk"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'litres F(11,5)', 'blob G', 'graphic P']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = dbf.Table('tempfp', ';'.join(fieldlist), dbf_type='vfp')
                table.close()
                table = dbf.Table('tempfp', dbf_type='vfp')
                actualFields = table.structure()
                table.close()
                yo.assertEqual(fieldlist, actualFields)
    def test05(yo):
        "vfp tables in memory"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'weight B', 'litres F(11,5)', 'int I', 'birth T', 'blob G', 'graphic P']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = dbf.Table(':memory:', ';'.join(fieldlist), dbf_type='vfp')
                actualFields = table.structure()
                table.close()
                yo.assertEqual(fieldlist, actualFields)
    def test06(yo):
        "vfp tables on disk"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'weight B', 'litres F(11,5)', 'int I', 'birth T', 'blob G', 'graphic P']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = dbf.Table('tempvfp', ';'.join(fieldlist), dbf_type='vfp')
                table.close()
                table = dbf.Table('tempvfp', dbf_type='vfp')
                actualFields = table.structure()
                table.close()
                yo.assertEqual(fieldlist, actualFields)
    def test07(yo):
        "dbf table:  adding records; adding and deleting fields"
        table = dbf.Table('temptable', 'name C(25); paid L; qty N(11,5); orderdate D; desc M', dbf_type='db3')
        namelist = []
        paidlist = []
        qtylist = []
        orderlist = []
        desclist = []
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            namelist.append(name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            record = table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc})
            last_byte = open(table.filename, 'rb').read()[-1]
            yo.assertEqual(last_byte, '\x1a')
            yo.assertEqual(record.name, name)
            yo.assertEqual(record.paid, paid)
            yo.assertEqual(record.qty, qty)
            yo.assertEqual(record.orderdate, orderdate)
            yo.assertEqual(record.desc, desc)
        for field in table.field_names:
            yo.assertEqual(1, table.field_names.count(field))
        table.close()
        last_byte = open(table.filename, 'rb').read()[-1]
        yo.assertEqual(last_byte, '\x1a')
        table = dbf.Table('temptable', dbf_type='db3')
        yo.assertEqual(len(table), len(floats))
        for field in table.field_names:
            yo.assertEqual(1, table.field_names.count(field))
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(table[i].name, namelist[i])
            yo.assertEqual(record.name, namelist[i])
            yo.assertEqual(table[i].paid, paidlist[i])
            yo.assertEqual(record.paid, paidlist[i])
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].orderdate, orderlist[i])
            yo.assertEqual(record.orderdate, orderlist[i])
            yo.assertEqual(table[i].desc, desclist[i])
            yo.assertEqual(record.desc, desclist[i])
            i += 1
        yo.assertEqual(i, len(table))
        table.delete_fields('name')
        table.close()
        table = dbf.Table('temptable', dbf_type='db3')
        for field in table.field_names:
            yo.assertEqual(1, table.field_names.count(field))
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(table[i].paid, paidlist[i])
            yo.assertEqual(record.paid, paidlist[i])
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].orderdate, orderlist[i])
            yo.assertEqual(record.orderdate, orderlist[i])
            yo.assertEqual(table[i].desc, desclist[i])
            yo.assertEqual(record.desc, desclist[i])
            i += 1
        table.delete_fields('paid, orderdate')
        for field in table.field_names:
            yo.assertEqual(1, table.field_names.count(field))
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].desc, desclist[i])
            yo.assertEqual(record.desc, desclist[i])
            i += 1
        yo.assertEqual(i, len(table))
        table.add_fields('name C(25); paid L; orderdate D')
        for field in table.field_names:
            yo.assertEqual(1, table.field_names.count(field))
        yo.assertEqual(i, len(table))
        i = 0
        for record in table:
            yo.assertEqual(record.name, '')
            yo.assertEqual(record.paid, False)
            yo.assertEqual(record.orderdate, datetime.datetime.now().date())
            i += 1
        yo.assertEqual(i, len(table))
        i = 0
        for record in table:
            record.name = namelist[record.record_number]
            record.paid = paidlist[record.record_number]
            record.orderdate = orderlist[record.record_number]
            record.write_record()
            i += 1
        yo.assertEqual(i, len(table))
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(table[i].name, namelist[i])
            yo.assertEqual(record.name, namelist[i])
            yo.assertEqual(table[i].paid, paidlist[i])
            yo.assertEqual(record.paid, paidlist[i])
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].orderdate, orderlist[i])
            yo.assertEqual(record.orderdate, orderlist[i])
            yo.assertEqual(table[i].desc, desclist[i])
            yo.assertEqual(record.desc, desclist[i])
            i += 1
        table.close()
    def test08(yo):
        "vfp table:  adding records; adding and deleting fields"
        table = dbf.Table('tempvfp', 'name C(25); paid L; qty N(11,5); orderdate D; \
                desc M; mass B; weight F(18,3); age I; meeting T; misc G; photo P', dbf_type='vfp')
        namelist = []
        paidlist = []
        qtylist = []
        orderlist = []
        desclist = []
        masslist = []
        weightlist = []
        agelist = []
        meetlist = []
        misclist = []
        photolist = []
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1, \
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            namelist.append(name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            masslist.append(mass)
            weightlist.append(weight)
            agelist.append(age)
            meetlist.append(meeting)
            misclist.append(misc)
            photolist.append(photo)
            record = table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc, \
                    'mass':mass, 'weight':weight, 'age':age, 'meeting':meeting, 'misc':misc, 'photo':photo})
            yo.assertEqual(record.name, name)
            yo.assertEqual(record.paid, paid)
            yo.assertEqual(record.qty, qty)
            yo.assertEqual(record.orderdate, orderdate)
            yo.assertEqual(record.desc, desc)
            yo.assertEqual(record.mass, mass)
            yo.assertEqual(record.weight, weight)
            yo.assertEqual(record.age, age)
            yo.assertEqual(record.meeting, meeting)
            yo.assertEqual(record.misc, misc)
            yo.assertEqual(record.photo, photo)
        table.close()
        table = dbf.Table('tempvfp', dbf_type='vfp')
        yo.assertEqual(len(table), len(floats))
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(table[i].name, namelist[i])
            yo.assertEqual(record.name, namelist[i])
            yo.assertEqual(table[i].paid, paidlist[i])
            yo.assertEqual(record.paid, paidlist[i])
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].orderdate, orderlist[i])
            yo.assertEqual(record.orderdate, orderlist[i])
            yo.assertEqual(table[i].desc, desclist[i])
            yo.assertEqual(record.desc, desclist[i])
            yo.assertEqual(record.mass, masslist[i])
            yo.assertEqual(table[i].mass, masslist[i])
            yo.assertEqual(record.weight, weightlist[i])
            yo.assertEqual(table[i].weight, weightlist[i])
            yo.assertEqual(record.age, agelist[i])
            yo.assertEqual(table[i].age, agelist[i])
            yo.assertEqual(record.meeting, meetlist[i])
            yo.assertEqual(table[i].meeting, meetlist[i])
            yo.assertEqual(record.misc, misclist[i])
            yo.assertEqual(table[i].misc, misclist[i])
            yo.assertEqual(record.photo, photolist[i])
            yo.assertEqual(table[i].photo, photolist[i])
            i += 1
        table.delete_fields('desc')
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(table[i].name, namelist[i])
            yo.assertEqual(record.name, namelist[i])
            yo.assertEqual(table[i].paid, paidlist[i])
            yo.assertEqual(record.paid, paidlist[i])
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].orderdate, orderlist[i])
            yo.assertEqual(record.orderdate, orderlist[i])
            yo.assertEqual(record.mass, masslist[i])
            yo.assertEqual(table[i].mass, masslist[i])
            yo.assertEqual(record.weight, weightlist[i])
            yo.assertEqual(table[i].weight, weightlist[i])
            yo.assertEqual(record.age, agelist[i])
            yo.assertEqual(table[i].age, agelist[i])
            yo.assertEqual(record.meeting, meetlist[i])
            yo.assertEqual(table[i].meeting, meetlist[i])
            yo.assertEqual(record.misc, misclist[i])
            yo.assertEqual(table[i].misc, misclist[i])
            yo.assertEqual(record.photo, photolist[i])
            yo.assertEqual(table[i].photo, photolist[i])
            i += 1
        table.delete_fields('paid, mass')
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(table[i].name, namelist[i])
            yo.assertEqual(record.name, namelist[i])
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].orderdate, orderlist[i])
            yo.assertEqual(record.orderdate, orderlist[i])
            yo.assertEqual(record.weight, weightlist[i])
            yo.assertEqual(table[i].weight, weightlist[i])
            yo.assertEqual(record.age, agelist[i])
            yo.assertEqual(table[i].age, agelist[i])
            yo.assertEqual(record.meeting, meetlist[i])
            yo.assertEqual(table[i].meeting, meetlist[i])
            yo.assertEqual(record.misc, misclist[i])
            yo.assertEqual(table[i].misc, misclist[i])
            yo.assertEqual(record.photo, photolist[i])
            yo.assertEqual(table[i].photo, photolist[i])
            i += 1
        table.add_fields('desc M; paid L; mass B')
        i = 0
        for record in table:
            yo.assertEqual(record.desc, '')
            yo.assertEqual(record.paid, False)
            yo.assertEqual(record.mass, 0.0)
            i += 1
        yo.assertEqual(i, len(table))
        i = 0
        for record in table:
            record.desc = desclist[record.record_number]
            record.paid = paidlist[record.record_number]
            record.mass = masslist[record.record_number]
            record.write_record()
            i += 1
        yo.assertEqual(i, len(table))
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            yo.assertEqual(table[i].name, namelist[i])
            yo.assertEqual(record.name, namelist[i])
            yo.assertEqual(table[i].paid, paidlist[i])
            yo.assertEqual(record.paid, paidlist[i])
            yo.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            yo.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            yo.assertEqual(table[i].orderdate, orderlist[i])
            yo.assertEqual(record.orderdate, orderlist[i])
            yo.assertEqual(table[i].desc, desclist[i])
            yo.assertEqual(record.desc, desclist[i])
            yo.assertEqual(record.mass, masslist[i])
            yo.assertEqual(table[i].mass, masslist[i])
            yo.assertEqual(record.weight, weightlist[i])
            yo.assertEqual(table[i].weight, weightlist[i])
            yo.assertEqual(record.age, agelist[i])
            yo.assertEqual(table[i].age, agelist[i])
            yo.assertEqual(record.meeting, meetlist[i])
            yo.assertEqual(table[i].meeting, meetlist[i])
            yo.assertEqual(record.misc, misclist[i])
            yo.assertEqual(table[i].misc, misclist[i])
            yo.assertEqual(record.photo, photolist[i])
            yo.assertEqual(table[i].photo, photolist[i])
            i += 1
        table.close()
    def test09(yo):
        "basic function tests - len, contains & iterators"
        table = dbf.Table('temptable', dbf_type='db3')
        for field in table.field_names:
            yo.assertEqual(1, table.field_names.count(field))
        for field in table.field_names:
            yo.assertEqual(field in table, True)
        length = sum([1 for rec in table])
        yo.assertEqual(length, len(table))
        i = 0
        for record in table:
            yo.assertEqual(record, table[i])
            i += 1

    def test10(yo):
        "basic function tests - top, bottom, next, prev, current, goto, delete, undelete"
        table = dbf.Table(':memory:', 'name C(10)', dbf_type='db3')
        yo.assertRaises(dbf.Bof, table.current)
        table.append()
        yo.assertEqual(table.current(), table[0])
        table = dbf.Table(':memory:', 'name C(10)', dbf_type='db3')
        table.append(multiple=10)
        yo.assertEqual(table.current(), table[0])
        table = dbf.Table('temptable', dbf_type='db3')
        total = len(table)
        table.bottom()
        yo.assertEqual(table.record_number, total)
        table.top()
        yo.assertEqual(table.record_number, -1)
        table.goto(27)
        yo.assertEqual(table.record_number, 27)
        table.goto(total-1)
        yo.assertEqual(table.record_number, total-1)
        table.goto(0)
        yo.assertEqual(table.record_number, 0)
        yo.assertRaises(IndexError, table.goto, total)
        yo.assertRaises(IndexError, table.goto, -len(table)-1)
        table.top()
        yo.assertRaises(dbf.Bof, table.prev)
        table.bottom()
        yo.assertRaises(dbf.Eof, table.next)
        for record in table:
            record.delete_record().write_record()
        table.use_deleted = False
        table.top()
        yo.assertRaises(dbf.Eof, table.next)
        table.bottom()
        yo.assertRaises(dbf.Bof, table.prev)
        table.use_deleted = True
        for record in table:
            record.undelete_record().write_record()

        # delete every third record
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            if i % 3 == 0:
                record.delete_record().write_record()
            i += 1
        #table.use_deleted(True)            should be default
        i = 0
        # and verify
        for record in table:
            yo.assertEqual(record.has_been_deleted, i%3==0)
            yo.assertEqual(table[i].has_been_deleted, i%3==0)
            i += 1

        # check that deletes were saved to disk..
        table.close()
        table = dbf.Table('temptable', dbf_type='db3')
        i = 0
        for record in table:
            yo.assertEqual(record.has_been_deleted, i%3==0)
            yo.assertEqual(table[i].has_been_deleted, i%3==0)
            i += 1

        # verify record numbers
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            i += 1

        # verify that deleted records are skipped
        i = 0
        table.use_deleted = False
        for record in table:
            yo.assertNotEqual(record.record_number%3, 0)
        table.goto(5)
        table.next()
        yo.assertEqual(table.record_number, 7)
        table.prev()
        yo.assertEqual(table.record_number, 5)

        # verify that deleted records are skipped in slices
        list_of_records = table[5:8]
        yo.assertEqual(len(list_of_records), 2)
        yo.assertEqual(list_of_records[0].record_number, 5)
        yo.assertEqual(list_of_records[1].record_number, 7)

        # verify behavior when all records are deleted
        for record in table:
            record.delete_record().write_record()
        table.bottom()
        yo.assertRaises(dbf.Eof, table.next)
        yo.assertEqual(table.eof(), True)
        table.top()
        yo.assertRaises(dbf.Bof, table.prev)
        yo.assertEqual(table.bof(), True)

        # verify deleted records are seen when .use_deleted is True
        table.use_deleted = True
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            i += 1

        # verify undelete using record
        table.use_deleted = False
        for i in range(len(table)):
            table.goto(i)
            record = table.current()
            yo.assertEqual(record.record_number, i)
            record.undelete_record().write_record()
            yo.assertEqual(record.has_been_deleted, False)
            yo.assertEqual(table[i].has_been_deleted, False)

        table.use_deleted = True
        # verify undelete using table[index]
        for record in table:
            record.delete_record().write_record()
        for i in range(len(table)):
            table.goto(i)
            record = table.current()
            yo.assertEqual(record.record_number, i)
            table[i].undelete_record().write_record()
            yo.assertEqual(record.has_been_deleted, False)
            yo.assertEqual(table[i].has_been_deleted, False)

        # verify all records have been undeleted (recalled)
        table.use_deleted = False
        i = 0
        for record in table:
            yo.assertEqual(record.record_number, i)
            i += 1
        yo.assertEqual(i, len(table))


    def test11(yo):
        "finding, ordering, searching"
        table = dbf.Table('temptable', dbf_type='db3')

        # find (brute force)
        unordered = []
        for record in table:
            unordered.append(record.name)
        for word in unordered:                                  # returns records
            records = table.query(python="name == %r" % word)
            yo.assertEqual(len(records), unordered.count(word))
            records = [rec for rec in table if rec.name == word]
            yo.assertEqual(len(records), unordered.count(word))

        # ordering by one field
        ordered = unordered[:]
        ordered.sort()
        name_index = table.create_index(lambda rec: rec.name)
        #table.index(sort=(('name', ), ))
        i = 0
        for record in name_index:
            yo.assertEqual(record.name, ordered[i])
            i += 1

        # search (binary)
        table.use_deleted = True
        for word in unordered:
            records = name_index.search(match=word)
            yo.assertEqual(len(records), unordered.count(word))
            records = table.query(python="name == %r" % word)
            yo.assertEqual(len(records), unordered.count(word))
            records = table.query("select * where name == %r" % word)
            yo.assertEqual(len(records), unordered.count(word))

        # ordering by two fields
        ordered = unordered[:]
        ordered.sort()
        nd_index = table.create_index(lambda rec: (rec.name, rec.desc))
        #table.index(sort=(('name', ), ('desc', lambda x: x[10:20])))
        i = 0
        for record in nd_index:
            yo.assertEqual(record.name, ordered[i])
            i += 1

        # search (binary)
        for word in unordered:
            records = nd_index.search(match=(word, ), partial=True)
            ucount = sum([1 for wrd in unordered if wrd.startswith(word)])
            yo.assertEqual(len(records), ucount)

        for record in table[::2]:
            record.qty = -record.qty
        unordered = []
        for record in table:
            unordered.append(record.qty)
        ordered = unordered[:]
        ordered.sort()
        qty_index = table.create_index(lambda rec: rec.qty)
        #table.index(sort=(('qty', ), ))
        i = 0
        for record in qty_index:
            yo.assertEqual(record.qty, ordered[i])
            i += 1
        for number in unordered:
            records = qty_index.search(match=(number, ))
            yo.assertEqual(len(records), unordered.count(number))

        table.close()
    def test12(yo):
        "scattering and gathering fields, and new()"
        table1 = dbf.Table('temptable', dbf_type='db3')
        table2 = table1.new('temptable2')
        for record in table1:
            newrecord = table2.append()
            testdict = record.scatter_fields()
            for key in testdict.keys():
                yo.assertEqual(testdict[key], record[key])
            newrecord.gather_fields(record.scatter_fields())
            for field in record.field_names:
                yo.assertEqual(newrecord[field], record[field])
            newrecord.write_record()
        table2.close()
        table2 = None
        table2 = dbf.Table('temptable2', dbf_type='db3')
        for i in range(len(table1)):
            dict1 = table1[i].scatter_fields()
            dict2 = table2[i].scatter_fields()
            for key in dict1.keys():
                yo.assertEqual(dict1[key], dict2[key])
            for key in dict2.keys():
                yo.assertEqual(dict1[key], dict2[key])
        table3 = table1.new(':memory:')
        for record in table1:
            newrecord = table3.append(record)
        table4 = dbf.Table('tempvfp', dbf_type='vfp')
        table5 = table4.new(':memory:')
        for record in table4:
            newrecord = table5.append(record)
        table1.close()
        table2.close()
    def test13(yo):
        "renaming fields, __contains__, has_key"
        table = dbf.Table('temptable', dbf_type='db3')
        for field in table.field_names:
            oldfield = field
            table.rename_field(oldfield, 'newfield')
            yo.assertEqual(oldfield in table, False)
            yo.assertEqual('newfield' in table, True)
            table.close()
            table = dbf.Table('temptable', dbf_type='db3')
            yo.assertEqual(oldfield in table, False)
            yo.assertEqual('newfield' in table, True)
            table.rename_field('newfield', oldfield)
            yo.assertEqual(oldfield in table, True)
            yo.assertEqual('newfield' in table, False)
        table.close()

    def test14(yo):
        "kamikaze"
        table1 = dbf.Table('temptable', dbf_type='db3')
        table2 = table1.new('temptable2')
        for record in table1:
            newrecord = table2.append(kamikaze=record)
            for key in table1.field_names:
                if not table1.is_memotype(key):
                    yo.assertEqual(newrecord[key], record[key])
            for field in newrecord.field_names:
                if not table2.is_memotype(field):
                    yo.assertEqual(newrecord[field], record[field])
        table2.close()
        table2 = dbf.Table('temptable2', dbf_type='db3')
        for i in range(len(table1)):
            dict1 = table1[i].scatter_fields()
            dict2 = table2[i].scatter_fields()
            for key in dict1.keys():
                if not table1.is_memotype(key):
                    yo.assertEqual(dict1[key], dict2[key])
            for key in dict2.keys():
                if not table2.is_memotype(key):
                    yo.assertEqual(dict1[key], dict2[key])
        table1.close()
        table2.close()

    def test15(yo):
        "multiple append"
        table1 = dbf.Table('temptable', dbf_type='db3')
        table2 = table1.new('temptable2')
        record = table1.next()
        table2.append(record.scatter_fields(), multiple=100)
        for samerecord in table2:
            for field in record.field_names:
                yo.assertEqual(record[field], samerecord[field])
        table2.close()
        table2 = dbf.Table('temptable2', dbf_type='db3')
        for samerecord in table2:
            for field in record.field_names:
                yo.assertEqual(record[field], samerecord[field])
        table2.close
        table3 = table1.new('temptable3')
        record = table1.current()
        table3.append(kamikaze=record, multiple=100)
        for samerecord in table3:
            for field in record.field_names:
                #if table3.is_memotype(field):
                #    yo.assertEqual('', samerecord[field])
                #else:
                    yo.assertEqual(record[field], samerecord[field])
        table3.close()
        table3 = dbf.Table('temptable3', dbf_type='db3')
        for samerecord in table3:
            for field in record.field_names:
                #if table3.is_memotype(field):
                #    yo.assertEqual('', samerecord[field])
                #else:
                    yo.assertEqual(record[field], samerecord[field])
        table3.close()
        table1.close()
    def test16(yo):
        "slices"
        table = dbf.Table('temptable', dbf_type='db3')
        slice1 = [table[0], table[1], table[2]]
        yo.assertEqual(slice1, list(table[:3]))
        slice2 = [table[-3], table[-2], table[-1]]
        yo.assertEqual(slice2, list(table[-3:]))
        slice3 = [record for record in table]
        yo.assertEqual(slice3, list(table[:]))
        slice4 = [table[9]]
        yo.assertEqual(slice4, list(table[9:10]))
        slice5 = [table[15], table[16], table[17], table[18]]
        yo.assertEqual(slice5, list(table[15:19]))
        slice6 = [table[0], table[2], table[4], table[6], table[8]]
        yo.assertEqual(slice6, list(table[:9:2]))
        slice7 = [table[-1], table[-2], table[-3]]
        yo.assertEqual(slice7, list(table[-1:-4:-1]))
    def test17(yo):
        "reset record"
        table3 = dbf.Table('temptable3', dbf_type='db3')
        for record in table3:
            record.reset_record()
            record.write_record()
        yo.assertEqual(table3[0].name, table3[1].name)
        table3[0].write_record(name='Python rocks!')
        yo.assertNotEqual(table3[0].name, table3[1].name)
    def test18(yo):
        "callable record"
        table = dbf.Table('temptable', dbf_type='db3')
        for record in table:
            yo.assertEqual(tuple([record.name]), record(('name',)))
            yo.assertEqual(tuple([record.name[5:]]), record(('name', lambda x: x[5:])))
            yo.assertEqual(tuple([record.name[:11]]), record(('name', lambda x: x[:11])))
            yo.assertEqual(tuple([record.name[3:9]]), record(('name', lambda x: x[3:9])))
            yo.assertEqual(tuple([record.qty]), record(('qty',)))
            yo.assertEqual(tuple([record.paid]), record(('paid',)))
            yo.assertEqual(tuple([record.desc]), record(('desc',)))
            yo.assertEqual(tuple([record.desc[:100]]), record(('desc', lambda x: x[:100])))
            yo.assertEqual(tuple([record.name, record.qty, record.desc[:25]]),
                    record(('name',), ('qty', ), ('desc', lambda x: x[:25])))
            yo.assertEqual(tuple([record.name, record.desc[4:6], record.paid]),
                    record(('name', ), ('desc', lambda x: x[4:6]), ('paid', )))
        table.close()
        table = dbf.Table('tempvfp', dbf_type='vfp')
        for record in table:
            yo.assertEqual(tuple([record.name]), record(('name', )))
            yo.assertEqual(tuple([record.name[5:]]), record(('name', lambda x: x[5:])))
            yo.assertEqual(tuple([record.name[:11]]), record(('name', lambda x: x[:11])))
            yo.assertEqual(tuple([record.name[3:9]]), record(('name', lambda x: x[3:9])))
            yo.assertEqual(tuple([record.qty]), record(('qty', )))
            yo.assertEqual(tuple([record.paid]), record(('paid', )))
            yo.assertEqual(tuple([record.desc]), record(('desc', )))
            yo.assertEqual(tuple([record.mass]), record(('mass', )))
            yo.assertEqual(tuple([record.weight]), record(('weight', )))
            yo.assertEqual(tuple([record.desc[:100]]), record(('desc', lambda x: x[:100])))
            yo.assertEqual(tuple([record.name, record.qty, record.desc[:25]]),
                    record(('name', ), ('qty', ), ('desc', lambda x: x[:25])))
            yo.assertEqual(tuple([record.name, record.desc[4:6], record.paid]),
                    record(('name', ), ('desc', lambda x: x[4:6]), ('paid', )))
        table.close()

    def test19(yo):
        "adding memos to existing records"
        table = dbf.Table(':memory:', 'name C(50); age N(3,0)', dbf_type='db3')
        table.append(('user', 0))
        table.add_fields('motto M')
        table[0].write_record(motto='Are we there yet??')
        yo.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
        table = dbf.Table('temptable4', 'name C(50); age N(3,0)', dbf_type='db3')
        table.append(('user', 0))
        table.add_fields('motto M')
        table[0].write_record(motto='Are we there yet??')
        yo.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
        table = dbf.Table('temptable4', dbf_type='db3')
        yo.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
    def test20(yo):
        "from_csv"
        table = dbf.Table('temptable')
        table.export(header=False)
        csvtable = dbf.from_csv('temptable.csv')
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable = dbf.from_csv('temptable.csv', to_disk=True, filename='temptable5')
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable = dbf.from_csv('temptable.csv', field_names=['field1','field2'])
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable = dbf.from_csv('temptable.csv', field_names=['field1','field2'], to_disk=True, filename='temptable5')
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable = dbf.from_csv('temptable.csv', extra_fields=['count N(5,0)','id C(10)'])
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable = dbf.from_csv('temptable.csv', extra_fields=['count N(5,0)','id C(10)'], to_disk=True, filename='temptable5')
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable = dbf.from_csv('temptable.csv', field_names=['name','qty','paid','desc'], extra_fields='test1 C(15);test2 L'.split(';'))
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable = dbf.from_csv('temptable.csv', field_names=['name','qty','paid','desc'], extra_fields='test1 C(15);test2 L'.split(';'), to_disk=True, filename='temptable5')
        for i in index(table):
            for j in index(table.field_names):
                yo.assertEqual(str(table[i][j]), csvtable[i][j])

class Test_Dbf_Lists(unittest.TestCase):
    "DbfList tests"
    def test01(yo):
        "addition and subtraction"
        table1 = dbf.Table('temptable', dbf_type='db3')
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        yo.assertEqual(100, len(table1))
        yo.assertEqual(list1[0], list2[0])
        yo.assertEqual(list1[3], list2[2])
        yo.assertEqual(50, len(list1))
        yo.assertEqual(34, len(list2))
        yo.assertEqual(33, len(list3))
        yo.assertEqual(117, len(list1) + len(list2) + len(list3))
        yo.assertEqual(len(table1), len(list1 + list2 + list3))
        yo.assertEqual(67, len(list1 + list2))
        yo.assertEqual(33, len(list1 - list2))
        yo.assertEqual(17, len(list2 - list1))
        table1.close()
    def test02(yo):
        "appending and extending"
        table1 = dbf.Table('temptable', dbf_type='db3')
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        list1.extend(list2)
        list2.append(table1[1])
        yo.assertEqual(67, len(list1))
        yo.assertEqual(35, len(list2))
        list1.append(table1[1])
        list2.extend(list3)
        yo.assertEqual(68, len(list1))
        yo.assertEqual(67, len(list2))
        table1.close()
    def test03(yo):
        "indexing"
        table1 = dbf.Table('temptable', dbf_type='db3')
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        for i, rec in enumerate(list1):
            yo.assertEqual(i, list1.index(rec))
        for rec in list3:
            yo.assertRaises(ValueError, list1.index, rec )
        table1.close()
    def test04(yo):
        "sorting"
        table1 = dbf.Table('temptable', dbf_type='db3')
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        list4 = table1[:]
        index = table1.create_index(key = lambda rec: rec.name )
        list4.sort(key=lambda rec: rec.name)
        for trec, lrec in zip(index, list4):
            yo.assertEqual(trec.record_number, lrec.record_number)
        table1.close()
unittest.main()
