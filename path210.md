# Notes For Model

Purpose: This file is a decision log used to compare a decision currently under consideration against past outcomes, so we can learn which factors drive right vs. wrong binary decisions. Every decision resolves later to "right" or "wrong".

1. The 'Past Events' section contains content split by 2 line breaks. Each chunk follows this schema:
name: string id starting with a number
result: either "right" or "wrong"
tags: single comma-separated line of strings (human-provided and model-generated tags are merged here, deduplicated into one normalized set)
line movement: optional single line, present only when timed numeric line values were provided. It is a comma-separated list of the line/price values in chronological order, each annotated with how long before the game it was observed (e.g. "-125 (2d), -128 (1d)"). If more than one market was tracked (e.g. a side and a total), separate them with a semicolon and label each (e.g. "side: ...; total: ..."). Times use d=days, h=hours, m=minutes before the game, plus "close" for the closing line.
context: string starting with "context" which is explanation of why the decision was made. It should capture the setup/reasoning for the decision, what moved (line/price), the action taken, the result, and the retrospective lesson (the "should have...").

2. tags: read each entry's content and generate/merge tags that make sense given all the other contexts. There is ONE tags line per entry — fold any human-supplied tags into the same normalized vocabulary rather than keeping a separate list. Prefer a consistent, reusable vocabulary so the same factor uses the same tag across entries (this is what makes patterns accumulate). Rules for generating/regenerating tags:
- Regenerate across the whole set, not just the new entry. When a new decision is logged, re-derive tags for ALL entries together so the vocabulary stays normalized.
- Rebuild the '# Model Cache' after any (re)generation, since its counts are derived from tags and must stay in sync.
- Only ever edit the tags line. Never modify name, result, or context — those are the source of truth.
- Tag format/quality: lowercase snake_case; each tag must be a reusable factor that could recur in a future decision (not a one-off description); never encode the outcome as a tag (no "wrong"/"loss"); consolidate near-duplicates into one canonical tag.
- Derive tags from context, independent of result, so tags describe the decision's factors rather than hindsight.

3. When asked to evaluate a decision under consideration:
- Extract its candidate factors and match them against past entries.
- Report the right/wrong track record of those shared factors (pull from '# Model Cache').
- Flag recurring factors associated with wrong decisions.
- Give a lean, including the reasoning behind it and whether it is a strong or weak lean.
- Record the lean, its reasoning, and its strength over time so the model's own decision-making can be evaluated too.
- Nervousness is not inherently a negative signal. The winning entries (5, 6) were both nervous_underdog_backing, so expect that the right amount of nervousness can correlate with good outcomes. When weighing a lean, treat the degree of nervousness as its own factor rather than a reason to avoid a bet — mild/healthy nervousness about a sound read can be a positive pattern, while its absence (overconfidence) has been a loss signal.
- Treat late line movement with skepticism for noise. Moves inside the last few hours before a game are often volatility, not signal. Weight the CLOSING line and the NET move (open -> close), not a transient intraday spike. A big-looking swing that reverts by close is effectively a stable line and should be tagged line_stable, NOT follow_/fade_line_movement — see entry 15, where mexico ran +104 open -> -104 (1h) but CLOSED +101, so the "-104 favorite" move was noise that should have been ignored. Do not overweight a move that closes near where it opened, and be most cautious about moves that only appear in the final hours.
- (Based on 20 logged events.) Line direction is the strongest signal in the log. Backing the side or total the market is moving TOWARD (follow_line_movement, 3/1) is the most reliable winning template — see entries 5, 6, 8, 19. The mirror is the losing shape: backing an underdog while the line moves toward the favorite has repeatedly failed (e.g. entry 18). fade_line_movement is 2/7 and has only won when a strong fundamental or health/class mismatch justified betting against the move (entries 14, 20) — do not fade movement on narrative alone.
- (Based on 20 logged events.) Treat chasing a bigger payout as a HIGH-ALERT warning. chased_better_payout is 0/6 — the worst record in the log. Taking a larger-return expression (a run line / big spread / parlay) over the simple moneyline or side has not cashed once (entries 3, 7, 9, 13, 17). Default to the simplest line expression; when a bigger-payout number is tempting, treat it as a strong signal to step back, since the extra payout reflects the added margin the market is pricing against you.
- (Based on 20 logged events.) Tipster signals have inverted in the log. followed_tipster is 0/4 (entries 1, 6, 13, 15) while faded_tipster is 2/1 (wins in entries 8, 14). Tailing a single tipster — especially one fresh off a win — has not cashed; fading one has a positive record. Treat "a tipster is on this" as a mild reason to fade rather than follow, not as a standalone signal. NOTE the difference between "a tipster" and "lots of tipsters": one tipster's pick is the faded_tipster/followed_tipster signal above, whereas broad agreement (many tipsters / the market consensus) is a SEPARATE factor tracked by follow_consensus (1/2) and fade_consensus (1/3), which are mixed and weaker — do not treat a single sharp's opinion and a crowd consensus as the same thing.
- (Based on 20 logged events.) Situational/narrative angles need line confirmation. situational_angle is 3/7 — it has only won when the angle ALIGNED with line movement or reflected a real class/health mismatch (entries 8, 19, 20). A pure narrative ("revenge spot", "should destroy", "shootout") with no line support has consistently lost (entries 9, 10, 13, 15, 16). CRUCIALLY, a narrative angle that points AGAINST the line movement is typically a wrong angle — the market moving the other way is evidence the narrative is mistaken (e.g. entry 18, where the "revenge spot" pointed one way but the line moved toward the other side and the angle lost). Require an angle to be backed by line direction or a concrete edge before weighting it; if the line disagrees with your narrative, trust the line.
- (Based on 20 logged events.) Distrust strong conviction formed days before with no line movement. overconfidence is 0/3 and decision_day_before is 1/4. High certainty locked in days out — especially "definitely the better team" / "bet of the tournament" reads — has underperformed, and when the line stays flat (line_stable, 0/2) there is no market confirmation of the edge (entry 10's lesson; also entries 12, 13, 15). A firm early opinion that the market never validates is a fade sign, not a green light.
- (Based on 20 logged events.) A favorite covering the spread/run line is NOT the same as a high-scoring game. In entry 16 the dodgers covered -1.5 (won by 3) but the total stayed under — a "should destroy / lots of runs" read conflated winning big with scoring a lot. Evaluate the side and the total as INDEPENDENT markets; a confident side read does not justify an over, and they can (and did) split. Moreover, a side lean and a total lean on the same game are unlikely to BOTH be right, so when you have a lean on both, look for line movement (or another distinguishing factor) on one of the two and prefer that market — the side/total split is itself a useful signal for which one to trust, rather than betting both.
- (Based on 20 logged events.) Take available hedges and avoid self-conflicting tickets. missed_hedge is 0/2 (entries 2, 7 — passing on an available ~50% hedge hurt) and parlay_conflict is 0/2 (entries 4, 13 — legs that fought each other or an existing position). When a reasonable mid-game or pre-game hedge is available, lean toward taking it, and avoid stacking a bet that conflicts with another position you already hold. Some games present a HIGH likelihood of an in-game hedge opportunity in the first half (which is ideal — an early swing lets you lock value): watch for high-emotion spots (a "game of the tournament", rivalry/revenge games, elimination matches) where volatile early scoring/live-line swings make a first-half hedge especially likely, and plan for it going in rather than reacting late.
- (Based on 24 logged events.) A reliable fade signal that ALSO has a Trent bet on it is a strong fade candidate. When a factor with a poor track record points against a bet AND Trent (a single tipster) is on that same bet, the two negatives compound. In entry 24 (fadeegypt) the -1.5 was a chased_better_payout (0/7, the worst tag) AND trent was on the -1.5 (followed_tipster 0/5) — both signals said fade, and the -1.5 lost. Treat "a known losing signal + a Trent pick on the same side" as a high-confidence spot to fade that side (or at minimum avoid backing it), since faded_tipster (2/1) beats followed_tipster (0/5) and stacking it on top of an already-weak factor makes the fade cleaner.

4. Maintain the '# Model Cache' section: for each factor keep a running right-vs-wrong record (counts). Track totals only, not streaks or consecutive patterns.

5. Continuation entries: a pending decision in '# Upcoming Events' may be extended over time with one or more '<name>_cont' blocks appended after the prior lean, each holding new information (line moves, tipster positions, time-to-event, etc.). When asked to update a lean, read the original entry plus ALL its '<name>_cont' blocks together, then write a fresh block at the bottom (e.g. 'updated_lean:' / 'final_lean:') with refreshed tags, direction, strength, and reasoning. Do not edit the earlier leans — append, so the evolution of the read is preserved.

# Past Events

1fadespain
wrong
fade_favorite,fade_line_movement,decision_day_before,failure_to_cash_out,followed_tipster,changed_mind
context: first play. wanted to fade favorite from day before because dubundo on it. morning of game: line movement towards favorite, took fade. movement in our favor. trent backs us. trent was off a good day, wanted to fade his first bet, ended up tailing it.  lost. should have cashed out when trent backed us.

2fadebtts
wrong
fade_consensus,outlier_price,missed_hedge
context: second play. wanted to fade absolute consensus on btts. betonline was so much higher than everyone -200 vs -130. first half no goals. second half both sides score.

3fadeperfectmlb
wrong
fade_consensus,fade_line_movement,envy_driven,chased_better_payout,live_loss_spiral,fear_of_numbers
third play. wanted to fade reisch on 4-0 day he had o8.5, he was on consensus & line movement side of mlb. i was staring at u9.5, it came to u9 better price so i jumped. instant loser to 11.5 live, 13.5, 17.5, 18.5, 19.5, 20.5, ended on 19. i was too scared of every number and thought 18.5 was the trap.

4fadelastfavorite
wrong
fade_favorite,gamblers_fallacy,tilt_bet,chased_better_payout,parlay_conflict
third play. previous day all favorites won, two close. convinced today a big upset was due. lost first two underdog bets. rage bet underdog in regulation instead of double chance because of amazing return. this was also third leg for kuku favorite parlay. underdog loses by 2.

5fadeengland
right
fade_favorite,fade_consensus,follow_line_movement,resisted_live_doubledown,nervous_underdog_backing
first play. consistent movement away from square position -2. wanted to wait for live better line, but took +1.5. dog takes early lead, wanted to live on -.5. favorite comes back to win by 1, so we win. but almost got tricked into live loss.

6fadeebelgium
right
fade_favorite,vibes_over_logic,abandoned_winning_method,fresh_off_win,resisted_live_doubledown,nervous_underdog_backing
second play. freshly off a win based on logic. same logic was pointing us to the favorite this time, but decided it made sense to go against the previous wininng method. almost took live double down when underdog was winning, but favorite tied at the very end then won in extra. we would have lost every live try.

7fadeegypt
wrong
back_favorite,follow_consensus,prefer_simple_line,chased_better_payout,price_deterioration,missed_hedge,extras_risk,greed_driven
context: reframed from a no-conviction underdog fade into backing egypt (favorite) to advance with consensus. correct play was the simple egypt-to-advance ~-155; instead got greedy and took regulation moneyline +140. skipped an available ~50% halftime hedge; underdog came back and ML ballooned +200 to +600. added a second egypt-to-advance bet at -143 when it later improved to -118. egypt advanced on penalties, so the advance thesis was right. lessons: don't lock a worse price (-143) when a better one (-118) may come; take the ~50% halftime hedge (unsure above 50% for soccer, need more data).

8fadecapeverde
right
back_favorite,follow_consensus,follow_line_movement,prefer_simple_line,spread_nervousness,extras_risk,avoided_payout_chase,faded_tipster
context: argentina heavy favorite; chose the moneyline over the -2 spread and avoided both the +100 nbtts/o1.5 parlay and trent's under 2.5 (trent was off a win). argentina won 3-2 in extra time. moneyline hit; the -2 would have lost (margin 1), the nbtts parlay lost (both scored), and trent's under lost (5 goals) — so every avoid was correct. the nervy 3-2/extra-time result validated the spread_nervousness and extras_risk flags: backing the moneyline over the spread was the right expression.

9fadeghana
wrong
back_favorite,follow_consensus,followed_tipster,spread_nervousness,chased_better_payout,nervous_winner,line_stable,situational_angle
line movement: colombia -1.5: +132 (9h), +132 (3h)
context: colombia favorite over ghana (ghana's insane home crowd), -1.5 at +132, line stable 9h/3h out. own read was a nervous narrow win ("colombia wins 1-0/2-0 late"), but wanted the -1.5 for the favorite anyway — reaching for the +132 payout over the simpler moneyline/under. trent was off 2 wins and on colombia -1.5. colombia won 1-0: the margin of 1 did not cover -1.5, so the spread lost (as did trent's -1.5). lesson: with a nervous_winner read of a one-goal game, taking a -1.5 that needs a 2-goal margin is chasing payout against your own read — the cover nervousness was real and should have pointed to the moneyline/under, not the spread.

10fadeliberty
wrong
back_favorite,situational_angle,spread_nervousness,overcaution,misread_line_movement
context: road favorite against a home-hangover spot; liked the angle but got nervous about the points and talked myself off it, reading the small -2.5 -> -2 drift as a fade_line_movement warning. liberty won by 13, so the spread cashed easily. the caution was the error: a minor line wiggle in a class mismatch was overweighted, and the sound situational read got passed. lesson: a tiny drift off a favorite is not a real fade signal; don't let points-nervousness kill a good angle. also having strong conviction day in advance without change in line movement is a sign for fade.

11fadesky
wrong
back_favorite,situational_angle,spread_confidence,fade_line_movement,motivated_underdog
line movement: chicago spread: -7 (9h), -4 (close)
context: backed chicago as the home favorite in a 'maybe motivated' spot, trusting the class gap and dismissing points fear. read -7 as stable 9h out, but the line drifted to -4 by close — the market was fading chicago. chicago lost by 8 outright. the closing drift (-7 -> -4) was the fade_line_movement warning i overlooked, and the 'maybe motivated' underdog risk materialized. lesson: track the CLOSING line, not just an early snapshot; an adverse drift off your favorite plus a live motivation angle is a real loss signal, not to be overridden by spread_confidence.

12fademorocco
wrong
fade_favorite,fade_consensus,fade_line_movement,faded_tipster,situational_angle,spread_nervousness,decision_day_before
line movement: morocco -.5: -125 (2d), -128 (1d)
context: morocco heavy home favorite -.5 with an insane home crowd; dabundo off a correct favorite call, trent on morocco -.5, and everyone was on morocco. the line firmed from -125 (2 days out) to -128 (1 day out) TOWARD morocco. the decision was to FADE the -.5 — bet against morocco covering (the draw/opponent side) — fading the consensus, the firming line, and both tipsters. morocco won 2-0, so the -.5 covered easily and the fade lost. lesson: fading a firming favorite that has unanimous consensus + line movement is the losing shape (same as entry 3) — this is a side to follow, not fade; the "value is thin" worry was not a reason to take the wrong side.

13fadeparaguay
wrong
back_favorite,chased_better_payout,prefer_simple_line,fade_line_movement,parlay_conflict,followed_tipster,decision_day_before,overconfidence
line movement: france -2: -101 (1d), +110 (2h), +119 (30m); o2.5+(-.5) parlay: +106 (1d), +116 (2h), +121 (30m)
context: france favorite over paraguay; naive read was "france scores 3 every game, easy -2" at -101, tempted toward more profit on an o2.5+(-.5) parlay (+106 -> +121). the -2 cover line drifted adversely all day (-101 -> +110 -> +119), the market progressively fading france covering. trent was off a win and on france -2.5 (an even harder cover). france won 1-0: it did not cover -2, the o2.5 under hit, so the straight -2, trent's -2.5 tail, and the parlay all would have lost. fading paraguay via the spread/parlay was incorrect. lesson: the adverse cover-line drift correctly predicted france would not cover; the payout chase and "scores 3" overconfidence were the losing shape. backing a favorite to WIN is not the same as covering a big number the market is actively fading — the fade_line_movement warning was right, and the disciplined pass would have avoided all three losing tickets.

14fadebrasil
right
fade_favorite,fade_line_movement,faded_tipster,situational_angle,spread_nervousness,decision_day_before
line movement: norway +.5: -102 (2d), +106 (1d), +109 (12h), +114 (3h), +112 (2h); total o2.5: -126 (2d), -128 (1d), -140 (12h), -150 (3h), -143 (2h)
context: fade of brasil (backing norway +.5) despite the market moving hard toward brasil — norway +.5 drifted -102 -> +112/+114 as money piled onto brasil, and BOTH tipsters (clown nick and trent) were on brasil -.5. the case for the fade was fundamental: an expert flagged a bad brasil team, and norway were the disrespected newcomer. backed norway +.5 against the line movement and against both tipsters. brasil lost 2-0, so norway won outright and the +.5 cashed easily. lesson: a strong fundamental/expert read against a weak favorite can beat heavy adverse line movement and tipster consensus — this is the FIRST fade_line_movement win in the log, so line movement is not automatically decisive when the fundamental case against the favorite is strong. (note: the model's pre-game lean was AGAINST this bet, over-weighting the 0/5 fade_line_movement signal — a miss to learn from.)

15fadeengland
wrong
fade_favorite,line_stable,followed_tipster,situational_angle,extras_risk,price_deterioration,decision_day_before,overconfidence
line movement: mexico to advance: +104->+108 (2d), +104 (1d), +108 (12h), +104 (6h), +100 (5h), -104 (1h), +101 (30m, close); total o2.5: +143 (1d), +143 (12h), +144 (6h), +144 (5h), +162 (1h)
context: "bet of the tournament" — faded england by backing mexico (home) to advance. mexico appeared to move from a +104/+108 dog to a -104 favorite at 1h out, and trent came on mexico -.5, which looked like the market and a tipster aligning with the bet; entry was taken at the shorter -104. BUT the line CLOSED at +101 — essentially back to the +104 open — so the net movement was negligible and the -104 was NOISE, not a real follow_line_movement signal (this was in truth a line_stable spot). england won the match 3-2, so mexico was eliminated and the advance bet lost; the total also blew well over (5 goals vs o2.5). lessons: (1) do not chase transient line swings in the last few hours — weight the CLOSING/net line; the -104 spike that reverted to +101 should have been ignored, and reading it as "money piling onto mexico" was reading noise; (2) taking a deteriorated -104 that then closed back at +101 gave up value for nothing on an overconfident "bet of the tournament". (note: BOTH model leans missed — the side lean backed mexico partly on the noisy -104 move, and the total lean was UNDER while 5 goals went over.)

16fadepadres
wrong
back_favorite,follow_line_movement,total_over,situational_angle,overconfidence,price_deterioration
line movement: dodgers -1.5: -105 (8h), -115 (30m); total: o9.5 -119 (8h), o10 -115 (30m)
context: "ohtani birthday so dodgers should destroy / lots of runs" narrative drove an over play (o9.5 -> o10) alongside a look at dodgers -1.5. the market confirmed both — the -1.5 firmed -105 -> -115 and the total climbed o9.5 -> o10 on over money (follow_line_movement), with entry at the worse numbers (o10 after o9.5). dodgers won 5-2: they DID cover -1.5 (won by 3), but only 7 total runs meant the total stayed UNDER 9.5/10, so the "lots of runs" over lost. lessons: an overconfident "should destroy / lots of runs" narrative (overconfidence + situational_angle) is a losing driver even when the favorite wins comfortably — a team covering the run line is NOT the same as a shootout total; and buying the deteriorated total (o10 after o9.5) added no value. (note: the model's total lean was a small OVER — wrong; the -1.5 caution was overly conservative since the -1.5 actually covered.)

17fademets
wrong
back_favorite,chased_better_payout,fade_line_movement,spread_nervousness
line movement: mets ML: -117 (10h), -112 (1h); mets -1.5: +155 (10h), +182 (1h)
context: mets a ~60% home favorite over a ~40% road dog. liked the ML at -117 but got pulled toward the -1.5 for the bigger payout (+155 -> +182). the market was COOLING on the mets all day — ML drifted -117 -> -112 and the run line ballooned +155 -> +182 (more plus money = the market moving off the -1.5). backed the mets anyway/chased the run-line payout. mets won 10-9: a 1-run win, so the -1.5 did NOT cover (and the 19-run game was a wild shootout). lesson: taking a bigger-payout run line (chased_better_payout, 0/6) into a cooling line (backing a favorite the market is fading = fade_line_movement) is the recurring losing shape — the 2-run cover nervousness was real; a 60/40 home edge at a deteriorating price/number had no value left.

18fadetigers
wrong
fade_favorite,fade_line_movement,situational_angle
line movement: texas ML: +103 (12h), +110 (3h)
context: backed home texas (~50% team) as the plus-money dog against the road tigers (~45% favorite) — a fade_favorite value spot, with a "tigers won yesterday, revenge/letdown" situational angle. but the texas dog price drifted +103 -> +110, i.e., money was moving TOWARD the tigers, so backing texas was against the movement (fade_line_movement). tigers won 6-3, so the texas dog lost. lesson: this repeats the losing template — backing a dog while the line moves toward the favorite is the wrong shape (the winning dog backs in entries 5/6 required the line moving TOWARD the dog); the fade-favorite value did not overcome adverse movement, and the situational angle actually pointed at the tigers.

19fadetempo
right
follow_line_movement,back_favorite,situational_angle
line movement: dallas spread: -5 (-105) (12h), -6 (-110) (3h); total: 183 (12h), 185 (3h)
context: wnba dallas markedly better on the road; read them to win by ~10. dallas -5 firmed to -6 (-105 -> -110), the market moving toward dallas exactly as the read expected (follow_line_movement), so backed dallas -6 rather than the under (the total ROSE 183 -> 185, so an under would have fought the move). dallas won by 13, covering -6 easily. lesson: this is the market-confirmed template — backing the favorite the line is moving toward (follow_line_movement, now 3/1, the best signal) beats a narrative under that fights a rising total; taking the side the movement confirms, not the market-contradicting total, was correct.

20fadeaces
right
fade_favorite,fade_line_movement,situational_angle
line movement: aces spread: fever +3 (-110) (12h), fever +3.5 (-115) (5h)
context: wnba vegas aces on a bad injury-hit stretch, fever well rested — situational read of a shootout/upset. backed the fever as the road dog (fade_favorite) at +3.5, across the key number of 3. the line had moved TOWARD the aces (fever +3 -> +3.5, i.e. aces -3 -> -3.5), so backing the fever was against the movement (fade_line_movement). aces lost by 16, so the fever covered easily and won outright. lesson: a genuine fundamental edge (injuries + rest mismatch) can win against adverse line movement — mirror of entry 18 (same fade_favorite + fade_line_movement + situational_angle shape, opposite result), showing that when the situational read is a real class/health mismatch (not just a narrative), fading the favorite into movement can still cash; getting the better side of the key number (+3.5) added margin.

21fadeportugal
right
back_favorite,fade_line_movement,follow_consensus,decision_day_before,overconfidence,situational_angle,total_under
line movement: spain -.5: -110 (2d), -112 (1d), -107 (12h), -107 (2h), -104 (10m, close); total u2.5: +104 (2d), +112 (1d), +104 (12h), +108 (2h), +116 (10m, close)
context: fade portugal = back spain -.5, a heavy favorite, on a "spain is definitely the better team" read locked in 2 days out. broad tipster consensus was on spain (trent, cblez, most tipsters). over the day the spain -.5 price CHEAPENED -110 -> -104 (money drifting OFF spain, toward portugal) and the u2.5 under drifted +104 -> +116 (money onto the OVER) — so BOTH markets' lines moved off the popular side. also considered u2.5 on a low-scoring narrative (portugal low scoring, spain doesn't score a lot). spain won 1-0: spain -.5 cashed AND u2.5 cashed (only 1 goal), so fading portugal was right and the under would have won too. lesson: broad consensus + a fundamental "clearly better team" read + a low-scoring narrative all came through DESPITE the line drifting off both popular sides — a fade_line_movement win (like entries 14, 20) where the popular/fundamental read beat the adverse late drift. this is a caution against mechanically fading a consensus-vs-line disconnect: when the fundamental case is strong, the late drift off the favorite can be noise/value, not a warning. (note: the final model lean was to PASS/fade BOTH markets, trusting the line drift over the consensus and the narrative — WRONG on both; the model over-weighted the consensus-vs-line disconnect and the fade_line_movement signal, the same class of miss as entries 14 and 20.)

22fadeusa
right
back_favorite,fade_line_movement,extras_risk,situational_angle,total_over
line movement: belgium to advance: +103 (3d), -110 (2d), +104 (1d), +113 (8h), +114 (1h); total o2.5: -132 (2d), -150 (1d), -152 (18h), -148 (8h), -130 (1h)
context: fade usa = back belgium to advance, belgium being the favorite with a comeback history, but off a lucky extra-time win over senegal (extras_risk). the to-advance price drifted OFF belgium on net (+103 -> +114 after a reverting -110 spike at 2d), i.e. money toward usa, so backing belgium was against the drift (fade_line_movement). tipsters were split — trent (off a win) on belgium, cblez on usa. belgium won 4-1: they advanced (side cashed) and the 5 goals cleared o2.5 (over cashed), so fading usa was right on both markets. lesson: a third straight fade_line_movement win (with 14, 20, 21) — a clearly-stronger favorite beat the adverse drift, and the extras_risk/entry-15 comparison did NOT bite because belgium was simply the better team and won decisively in regulation, not a nervy 1-1. the late-reverting -110 spike was correctly read as noise. (note: the model lean was PASS/cautious on belgium and weak/pass the over — WRONG on both; it over-weighted the drift + extras_risk, the same class of miss as entries 14, 20, 21.)

23fadevalkyries
wrong
fade_favorite,fade_line_movement,situational_angle,total_over
line movement: valkyries spread: -5.5 (-110) (1d), -5.5 (-115) (6h), -6.5 (-112) (30m); total: o156 (-110) (1d), o156 (-105) (6h), o155 (-110) (30m)
context: fade the valkyries = back the home dog +6.5 (read: "road favorite should be a small dog"), plus an over lean ("o156 too low for talented teams"). both instincts fought the line: valkyries -5.5 firmed to -6.5 (market moving TOWARD the favorite) and the total ticked DOWN 156 -> 155 (money on the under). valkyries won 62-49 (by 13), so the home dog +6.5 lost AND the total of 111 blew way under 155, so the over lost too — both wrong. lesson: textbook narrative-vs-line loss — backing a home dog while the line moves toward the favorite breaks the winning dog template (entries 5, 6, which needed the line moving TOWARD the dog), and backing an over while the total drops fights the move; trusting the line (valkyries side + the under) would have been right on both. (note: the model lean correctly warned AGAINST both markets — a model hit, the disciplined "trust the line over the narrative" read paid off.)

24fadeegypt
wrong
back_favorite,follow_line_movement,chased_better_payout,price_deterioration,followed_tipster,situational_angle,total_over
line movement: argentina -1.5: +124 (3d), +123 (2d), +123 (1d), +111 (12h), -101 (1h, close); total o2.5: +108 (3d), +108 (2d), -101 (1d), +102 (12h), -103 (1h, close)
context: fade egypt = back argentina -1.5, expecting a blowout. the -1.5 firmed hard all day (+124 -> -101 close), money piling ONTO the cover (follow_line_movement), and trent came on argentina -1.5; entry was the -1.5 at the deteriorated price. argentina won 3-2 — a 1-goal win, so egypt covered +1.5 and the -1.5 did NOT cover; fading egypt was wrong. the total (5 goals) cleared o2.5, so the small over would have won. lesson: this is the entry-9 trap again — follow_line_movement correctly predicted argentina would WIN, but a -1.5 needs a 2-goal margin, and a favorite winning is NOT the same as covering a big spread; the line confirming the SIDE does not justify the payout-chase expression (chased_better_payout 0/6), and buying the -1.5 at -101 after +124 was price_deterioration (paying up for a move that already happened). the moneyline was the correct expression of the confirmed read. (note: the model lean was a HIT on process — it explicitly preferred the ML over the -1.5, warned against paying -101, flagged the chased_better_payout trap, and called the small over that won; the placed -1.5 lost exactly as the lean cautioned.)

# Model Cache

Signal right/wrong record (based on tags):
follow_line_movement: 3 right / 2 wrong
resisted_live_doubledown: 2 right / 0 wrong
nervous_underdog_backing: 2 right / 0 wrong
fade_favorite: 4 right / 6 wrong
faded_tipster: 2 right / 1 wrong
vibes_over_logic: 1 right / 0 wrong
abandoned_winning_method: 1 right / 0 wrong
fresh_off_win: 1 right / 0 wrong
avoided_payout_chase: 1 right / 0 wrong
extras_risk: 2 right / 2 wrong
follow_consensus: 2 right / 2 wrong
prefer_simple_line: 1 right / 2 wrong
spread_nervousness: 2 right / 4 wrong
fade_consensus: 1 right / 3 wrong
situational_angle: 5 right / 9 wrong
decision_day_before: 2 right / 4 wrong
fade_line_movement: 4 right / 8 wrong
back_favorite: 4 right / 8 wrong
chased_better_payout: 0 right / 7 wrong
followed_tipster: 0 right / 5 wrong
missed_hedge: 0 right / 2 wrong
parlay_conflict: 0 right / 2 wrong
nervous_winner: 0 right / 1 wrong
line_stable: 0 right / 2 wrong
failure_to_cash_out: 0 right / 1 wrong
changed_mind: 0 right / 1 wrong
outlier_price: 0 right / 1 wrong
envy_driven: 0 right / 1 wrong
live_loss_spiral: 0 right / 1 wrong
fear_of_numbers: 0 right / 1 wrong
gamblers_fallacy: 0 right / 1 wrong
tilt_bet: 0 right / 1 wrong
price_deterioration: 0 right / 4 wrong
greed_driven: 0 right / 1 wrong
overcaution: 0 right / 1 wrong
misread_line_movement: 0 right / 1 wrong
spread_confidence: 0 right / 1 wrong
motivated_underdog: 0 right / 1 wrong
overconfidence: 1 right / 3 wrong
total_over: 1 right / 3 wrong
total_under: 1 right / 0 wrong

# Upcoming Events
fadeswiss
3 days out, the favorite colombia -.5 +130 is great value for home field advantage for the better team. 3 days out, tempting to get high return on o2.5 +146. 2 days out, the favorite is -.5 +130 and the total is o2.5 +146. 1 day out, the favorite is -.5 +130 and the total is o2.5 +146. 12 hours out, the favorite is -.5 +129 and the total is o2.5 +134. 5 hours out, the favorite is -.5 +129 and the total is o2.5 +136. 2 hours out, the favorite is -.5 +130 and the total is o2.5 +136.
final_lean:
tags: back_favorite, line_stable, follow_line_movement, chased_better_payout, decision_day_before, overconfidence, situational_angle, total_over
<ins>direction: TOTAL (o2.5) is the better side, but only mildly — the over firmed +146 -> +136 (some money onto the over) yet STALLED, so a small over is defensible, not a conviction play. SIDE (colombia -.5): weak/small — +130 for the better team to win is fair value but the line is FLAT (+130 -> +130), no confirmation, on a "great value" read locked in 3 days out.</ins>
strength: weak-moderate on the over, weak/small on the side.
reasoning: on the SIDE, colombia -.5 held +130 (3d) -> +130 (2d) -> +130 (1d) -> +129 (12h) -> +129 (5h) -> +130 (2h): dead flat = line_stable (0/2), so backing colombia is a back_favorite (4/8) with NO market confirmation, and the "great value... better team" read was locked in 3 days out = decision_day_before (2/4) + overconfidence (1/3) — the same flat-line/day-before shape as fadeportugal's side and the fadeegypt caution. +130 for colombia to win outright (the -.5 only needs a win) is fair IF they are genuinely better and at home, so it is a small fair-value dart, not an edge. On the TOTAL, o2.5 went +146 (3d) -> +146 -> +146 -> +134 (12h) -> +136 (5h) -> +136 (2h): the over firmed from +146 to ~+135 (money onto the over = follow_line_movement, the best signal at 3/2) but then PLATEAUED and even ticked back slightly, so the move is real but has stalled — not a continuing firm. That makes it a modest over, and since it is still a high-payout number, respect chased_better_payout (0/7) and total_over (1/3): keep it small. Per the notes, side and total are unlikely to both be right, and the line movement (mild as it is) favors the OVER over the flat side. net: a small over on the mild line confirmation is the cleaner play, plus a small colombia-to-win dart as fair value — do not oversize either, especially the flat-line favorite.