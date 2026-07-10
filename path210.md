# Notes For Model

Purpose: This file is a decision log used to compare a decision currently under consideration against past outcomes, so we can learn which factors drive right vs. wrong binary decisions. Every decision resolves later to "right" or "wrong".

1. The 'Past Events' section contains content split by 2 line breaks. Each chunk follows this schema:
name: string id starting with a number
result: either "right" or "wrong"
tags: single comma-separated line of strings (human-provided and model-generated tags are merged here, deduplicated into one normalized set)
line movement: optional single line, present only when timed numeric line values were provided. It is a comma-separated list of the line/price values in chronological order, each annotated with how long before the game it was observed (e.g. "-125 (2d), -128 (1d)"). If more than one market was tracked (e.g. a side and a total), separate them with a semicolon and label each (e.g. "side: ...; total: ..."). Times use d=days, h=hours, m=minutes before the game, plus "close" for the closing line.
context: string starting with "context" which is explanation of why the decision was made. It should capture the setup/reasoning for the decision, what moved (line/price), the action taken, the result, and the retrospective lesson (the "should have...").
model_lean: optional single line, present when the decision came from '# Upcoming Events' and the model had recorded a lean. Briefly summarize the model's pre-game lean per market — side, total, parlay, or whatever markets were in the pick — each with its direction, strength (strong/moderate/small/pass), and whether it HIT or MISSED once resolved. This captures model-lean accuracy in a compact, structured form; the prose context lesson can still elaborate.

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
- (Based on 20 logged events.) Treat chasing a bigger payout as a HIGH-ALERT warning. chased_better_payout is 0/6 — the worst record in the log. Taking a larger-return expression (a run line / big spread / parlay) over the simple moneyline or side has not cashed once (entries 3, 7, 9, 13, 17). Default to the simplest line expression; when a bigger-payout number is tempting, treat it as a strong signal to step back, since the extra payout reflects the added margin the market is pricing against you. NOTE the mirror-image trap, which is DISTINCT from chasing payout: OVER-INSURING a confirmed side by paying up for a safer cushion — e.g. laying heavy juice for a -1.5/+1.5 spread (paying -158 or -195) instead of taking the moneyline. Chasing payout reaches for a BIGGER return (a plus-money longshot expression); over-insuring accepts a WORSE return (heavy minus juice) to reduce variance. Both give back the edge and both are wrong defaults, but do not tag or describe over-insurance as chased_better_payout — it is the opposite direction. When the read is that a side simply WINS, the moneyline is the correct expression: don't reach past it for a bigger-payout spread (chased_better_payout), and don't pay up past it for a safety cushion (over-insurance) either.
- (Based on 20 logged events.) Tipster signals have inverted in the log. followed_tipster is 0/4 (entries 1, 6, 13, 15) while faded_tipster is 2/1 (wins in entries 8, 14). Tailing a single tipster — especially one fresh off a win — has not cashed; fading one has a positive record. Treat "a tipster is on this" as a mild reason to fade rather than follow, not as a standalone signal. NOTE the difference between "a tipster" and "lots of tipsters": one tipster's pick is the faded_tipster/followed_tipster signal above, whereas broad agreement (many tipsters / the market consensus) is a SEPARATE factor tracked by follow_consensus (1/2) and fade_consensus (1/3), which are mixed and weaker — do not treat a single sharp's opinion and a crowd consensus as the same thing.
- (Based on 20 logged events.) Situational/narrative angles need line confirmation. situational_angle is 3/7 — it has only won when the angle ALIGNED with line movement or reflected a real class/health mismatch (entries 8, 19, 20). A pure narrative ("revenge spot", "should destroy", "shootout") with no line support has consistently lost (entries 9, 10, 13, 15, 16). CRUCIALLY, a narrative angle that points AGAINST the line movement is typically a wrong angle — the market moving the other way is evidence the narrative is mistaken (e.g. entry 18, where the "revenge spot" pointed one way but the line moved toward the other side and the angle lost). Require an angle to be backed by line direction or a concrete edge before weighting it; if the line disagrees with your narrative, trust the line.
- (Based on 20 logged events.) Distrust strong conviction formed days before with no line movement. overconfidence is 0/3 and decision_day_before is 1/4. High certainty locked in days out — especially "definitely the better team" / "bet of the tournament" reads — has underperformed, and when the line stays flat (line_stable, 0/2) there is no market confirmation of the edge (entry 10's lesson; also entries 12, 13, 15). A firm early opinion that the market never validates is a fade sign, not a green light.
- (Based on 34 logged events.) Be cautious with a point-total OVER, especially in the WNBA. This is not a claim that line drift is meaningless — just that overs carry extra downside risk from how games end: WNBA endgames can strand an over (intentional fouling, deliberate clock-milking, fewer possessions, garbage-time bench lineups in a blowout) so the last few minutes may produce almost no points. In entry 34 (fadestorm) the game landed at 167, under a total that had drifted up to u170. total_over is 4/7. Practical rule: when you have a lean on BOTH the side and the total of the same game and must pick one (they rarely both cash), lean toward the side and away from the over — treat a shaky over as the leg to drop, particularly in low-possession or likely-blowout WNBA spots.
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

25fadeswiss
wrong
back_favorite,fade_line_movement,line_stable,chased_better_payout,decision_day_before,overconfidence,situational_angle,total_over,followed_tipster
line movement: colombia -.5: +130 (3d), +130 (2d), +130 (1d), +129 (12h), +129 (5h), +133 (2h), +146 (30m, close); total o2.5: +146 (3d), +146 (2d), +146 (1d), +134 (12h), +136 (5h), +136 (2h), +147 (30m, close)
context: fade swiss = back colombia -.5, a "better team with home-field value" read locked in 3 days out, plus a tempting high-return o2.5 +146. over the days colombia's -.5 price drifted UP +130 -> +146 (money coming OFF colombia, so backing the favorite was AGAINST the move = fade_line_movement), and the o2.5 over round-tripped +146 -> +134 (12h) -> +147 (close), reverting to flat = effectively line_stable, not a real over signal. trent (off a LOSS) was on both-teams-to-score (BTTS). the game finished 0-0 through regulation/extra time and switzerland won on penalties, so colombia -.5 did NOT cash, the o2.5 stayed well under (0 goals), and trent's BTTS lost too — fading swiss was wrong on both our markets. lesson: a fade-against-the-line + payout-chase miss — backing colombia while the price drifted off it (fade_line_movement 4/8) on a day-before "better team" read (decision_day_before, overconfidence) with no market confirmation (the over reverted to line_stable) failed, and the high-return o2.5 was another chased_better_payout (0/7) miss. crucially, trent was off a LOSS and holding BTTS, and BTTS in a low-scoring reverting market was exactly the tipster signal to FADE — in retrospect we should have faded trent's BTTS (faded_tipster 2/1 beats followed_tipster 0/5), and fading it would have cashed on the 0-0. (note: the final model lean was PASS the reverted over and only a SMALL colombia-to-win dart at the improved +146 — the small colombia play lost, but the model correctly passed the round-tripped over and sized the side down: a partial process hit.)

26fadepirates
wrong
fade_favorite,follow_line_movement,situational_angle,overconfidence,chased_better_payout,total_over
line movement: road dog +1.5: -140 (open), -152 (7h), -153 (6h), -158 (3h); moneyline: +149 (open), +133 (7h), +128 (6h), +129 (3h); total o8: -105 (open), -110 (7h), -110 (6h), -113 (3h)
context: fade pirates = back the road dog (the 60% win-rate team, a road underdog to the 50% home pirates) on a "should destroy in a high-scoring game" read. all three markets firmed toward the dog: the +1.5 -140 -> -158, the moneyline +149 -> +129, and the o8 over juice -105 -> -113 — money piled onto the dog to cover, to win outright, and onto the over (follow_line_movement, the best signal). the entry leaned the dog, tempted by the +1.5 at the deteriorated -158 (chased_better_payout) plus a small over. pirates won 12-4 (by 8): the road dog lost outright, so the moneyline AND the +1.5 both lost, and fading pirates was wrong. the 16 total runs cleared o8, so the small over WON. lesson: follow_line_movement is the best signal but not infallible — here the market firmed hard toward the dog on ALL three markets and the dog still got blown out by 8, a reminder that the line confirming a side is not a guarantee, especially when the underlying read is an overconfident "should destroy" narrative (overconfidence, situational_angle) rather than a real class/health edge. no expression of the side would have cashed (even the moneyline lost, so the +1.5 -158 payout-chase was moot), and the only winner was the over, which the market also pointed to. (note: the model lean was FOR the road dog on the MONEYLINE (moderate-strong) plus a small over — WRONG on the side it emphasized, RIGHT on the small over; a clean follow_line_movement miss where the confirmed side lost outright.)

27fadeliberty
right
back_favorite,situational_angle,fade_line_movement,line_stable,decision_day_before,overconfidence,total_under,spread_nervousness
line movement: dallas spread: -5 (-107) (1d), -4 (-110) (9h), -4.5 (-110) (30m); total: u175.5 (1d), u175 (-115) (9h), u176 (-112) (30m)
context: fade liberty = back the hot dallas team as a road favorite laying points into a liberty home-hangover spot, plus a "good defense" under lean. dallas's spread drifted -5 -> -4 -> -4.5 (a small move off dallas that gave a better number) and the total round-tripped u175.5 -> u175 -> u176. liberty lost 88-77: dallas won by 11 and comfortably covered -4.5, so fading liberty was right; the 165 total also stayed under u176. lesson: a clean repeat of the entry-10 template — a road favorite in a home-hangover spot covered, and the tiny drift off the favorite (per entry 10's lesson) was correctly ignored rather than faded; the situational angle backed by the right shape delivered.
model_lean: side (dallas -4.5) — moderate LEAN FOR, HIT (covered by 11); total (u176) — pass/small lean, HIT (165 stayed under); no parlay.

28fadesky
wrong
back_favorite,line_stable,fade_line_movement,situational_angle,overconfidence,total_over
line movement: home favorite spread: -3 (-112) (1d), -3.5 (-113) (11h), -3 (-104) (8h), -3 (-115) (6h), -3 (-115) (3h); total: o176.5 (-105) (1d), o172.5 (-110) (11h), o172.5 (-110) (8h), o172.5 (-105) (6h), o172.5 (-108) (3h)
context: fade sky = back the "better home team" -3 to cover a short spread in a "high scoring game", plus an over lean. the spread held essentially flat at -3 (a brief -3.5 blip at 11h reverted) = line_stable with no market confirmation, while the total DROPPED 176.5 -> 172.5 and stayed there for 8+ hours = money firmly onto the UNDER, directly contradicting the "high scoring" narrative. sky won 77-66 (by 11): the home favorite lost outright, so the -3 did NOT cover, and the 143 total finished well under o172.5 — fading sky was wrong on the side and the over would have lost too. lesson: a textbook overconfident-narrative-vs-line loss — an unconfirmed flat-line favorite (back_favorite, line_stable) on a "should easily cover" read (overconfidence) got blown out, and the "high scoring" over fought a clear, sustained 4-point drop to the under (fade_line_movement) and was never live. trusting the line (which said flat side / firm under) over the narrative would have avoided both.
model_lean: side (home favorite -3) — weak/small lean (flagged flat line, no edge), HIT direction (advised small/pass; the -3 lost); total (over) — PASS/fade lean, HIT (143 well under, over lost); no parlay.

29fadepirates
right
fade_favorite,fade_line_movement,situational_angle,overconfidence,gamblers_fallacy,total_over
line movement: braves +1.5: -195 (1d), -192 (2h), -188 (30m), -192 (10m); braves ML: +104 (1d), +103 (2h), +109 (30m), +110 (10m); total: o8.5 -116 (1d), o9 -124 (2h), o9 -126 (10m)
context: fade pirates = back the road dog braves (the better-recorded team) to win and score a lot, on a "no way they lose back to back" read. into the game the braves ML actually LENGTHENED +104 -> +110 (money coming OFF braves to win) and the +1.5 stayed heavy around -192 (no firming toward the cover), while the total rose o8.5 -> o9 with the over juice firming to -126. braves won 3-0: the road dog won outright, so fading pirates was right on the SIDE (ML and +1.5 both cashed), but the 3 total runs finished well UNDER o9, so the over LOST. lesson: a fade_line_movement WIN on the side — the market drifted off braves all day yet the better-record road dog won a shutout, so the "won't lose back to back" instinct came through despite no line support (like entries 14, 20, 21, 22). but a 3-0 shutout is the opposite of "score a lot," so the high-scoring half of the thesis busted even though the over juice had firmed — side and total split again, and the over move was noise here.
model_lean: side (braves ML/+1.5) — PASS/fade lean, MISS (braves won outright, the faded side cashed); total (over o9) — small lean FOR, MISS (3-0 shutout, well under); no parlay. a double model miss on a fade_line_movement result the model trusted the line into.

30fadevalkyries
wrong
fade_favorite,fade_line_movement,situational_angle,overconfidence,total_under
line movement: valkyries spread: -7.5 (-105) (1d), -8 (-115) (6h), -8 (-107) (2h), -8 (-115) (1h), -8.5 (-110) (30m); total: o168.5 (-105) (1d), o166 (-105) (6h), o165.5 (-110) (2h), o165.5 (-105) (1h), o165.5 (-105) (30m)
context: fade valkyries = back the home dog (+7.5 -> +8.5) on an "overrated road favorite loses a close low-scoring game" read, plus an under lean. the line firmed toward the road FAVORITE all day (-7.5 -> -8 -> -8.5, money onto valkyries) and the total dropped 168.5 -> 165.5 and held. valkyries won 83-75 (by 8): the home dog did NOT cover (lost by 8 > the +7.5 taken), so fading valkyries was wrong on the side; the 158 total finished UNDER 165.5, so the under WOULD have won. lesson: textbook narrative-vs-line loss on the side — backing a home dog while the line moves TOWARD the favorite is the entry-23 losing shape, and the favorite duly won by 8; meanwhile the under (which followed the line's 3-point drop down) was the correct read, so the line was right on both the side (favorite) and the total (under).
model_lean: side (home dog +7.5/+8.5) — PASS/fade lean, HIT (home dog lost, faded correctly); total (under 165.5) — small lean FOR, HIT (158 under); no parlay. a double model hit trusting the line over the narrative.

31fadesun
wrong
back_favorite,fade_line_movement,situational_angle,overconfidence,chased_better_payout,total_over
line movement: road favorite spread: -7 (-117) (1d), -6.5 (-110) (6h), -6.5 (-108) (2h); total: o168.5 (-108) (1d), o166.5 (-110) (6h), o166.5 (-110) (2h)
context: fade sun = back the road favorite (-7 -> -6.5) to smash sun and cover big in a high-scoring game, on a revenge read off a prior one-point home upset, plus an over lean. the line came OFF the favorite -7 -> -6.5 (money toward sun/the home dog) and the total DROPPED 168.5 -> 166.5, both against the thesis. sun lost 86-80 (by 6): the road favorite won but by only 6, so it did NOT cover -6.5, and the 166 total finished UNDER o166.5 — so fading sun was wrong on the side and the over lost too. lesson: the entry-28 fadesky template again — a "should get smashed in a shootout" narrative that the line contradicted on BOTH markets, and both halves lost: laying -6.5 for a big cover (chased_better_payout) when the number was shrinking was backwards, and the "high scoring" over fought a clear drop to the under. trusting the line (home dog + under) over the narrative was right on both.
model_lean: side (road favorite -6.5) — PASS/fade lean, HIT (favorite won by only 6, did not cover); total (over) — PASS/fade lean, HIT (166 under o166.5); no parlay. a double model hit trusting the line over the narrative.

32fadefever
right
fade_favorite,fade_line_movement,situational_angle,overconfidence,motivated_underdog,total_over
line movement: sparks (home dog): +6.5 (-108) (1d), +5.5 (-108) (5h), +6 (-110) (3h), +5.5 (-105) (10m); total: o185 (-105) (1d), o182.5 (-110) (9h), o182 (-110) (5h), o182 (-110) (10m)
context: fade fever = back the home dog sparks (+6.5) and the over, on a "weird for a supposedly bad team to lay this many points on the road, should be close and high scoring" read (fever = road favorite laying the points). into the game the number firmed toward fever (sparks +6.5 -> +5.5, money onto the favorite to cover) and the total dropped 185 -> 182, both against the thesis. fever lost 106-92: the home dog sparks won outright by 14, so the +6.5 cashed easily and fading fever was right; the 198 total sailed WAY OVER o182, so the over cashed too — right on both. lesson: a strong fade_line_movement WIN — the closing line was badly wrong on both markets (it firmed toward fever and dropped the total), yet the home dog blew fever out by 14 and the game blew over. the "weird for a bad team to lay this many points" instinct correctly distrusted the market's respect for fever, and both the dog and the over beat the line (like entries 14, 20, 21, 22).
model_lean: side (home dog sparks +6.5) — PASS/mild-fade lean, MISS (sparks won outright by 14); total (over o182) — PASS/fade lean, MISS (198, way over); no parlay. a double model miss on a fade_line_movement result the model trusted the line into.

33fademorocco
right
back_favorite,follow_line_movement,follow_consensus,situational_angle,overconfidence,decision_day_before,total_over
line movement: france -1: +101 (5d), +102 (4d), +102 (3d), +108 (2d), +102 (1d), +104 (3h), +112 (1h), +107 (90m), +100 (30m), +100 (20m); total o2.5: +101 (4d), +103 (3d), +101 (2d), +100 (1d), +108 (3h), -102 (1h), -112 (30m), -110 (20m)
context: fade morocco = back france -1, a "best team of the tournament, should win by at least 2 so worst case a push" read locked 5 days out. the france price sat flat/pick'em for days (+101 -> +112) then TIGHTENED onto france in the last ~90 minutes (+112 -> +100) as broad tipster consensus came in on france; the total firmed onto the over (+108 -> -110) and trent was on the under u2.5. also tempting was a parlay of france -.5 with mbappe shots-on-goal -334 to reach a -114 payout instead of risking a push. france won 2-0: france -1 covered (won by exactly 2), so fading morocco was RIGHT on the side; but 2 goals stayed UNDER o2.5, so the over LOST and trent's under won. lesson: the key lesson is about what the fade/keep-it-close signals implicitly ASSUME. all the "fade the favorite / take the dog to keep it close" reasoning is a bet on the favorite UNDERPERFORMING and the underdog OVERPERFORMING relative to their averages — a dangerous default to tie every lean to. here neither team played above or below their average: the better team simply won by its expected margin, so the correct play was to TAKE THE FAVORITE at face value, not to back the dog to stay close. also a clean instance of the flagged side/total conflict: france winning by exactly 2 (2-0) is precisely the outcome that COVERS -1 while staying UNDER 2.5, so backing both france -1 and the over could never both cash — the premise ("win by 2") actually pointed at the under all along.
model_lean: side (france -1) — SMALL LEAN FOR (upgraded on late money onto france + consensus), HIT (covered by 2); total (over o2.5) — small-moderate lean FOR (primary), MISS (2-0 under; note the model explicitly flagged this exact side/total conflict beforehand); parlay (france -.5 + mbappe -334) — advised AVOID (france -.5 would have won, but the model correctly refused to manufacture payout with a longshot leg). net: model HIT the side it upgraded and correctly pre-called the conflict that sank the over. SIGNAL TO TRACK: the side lean was TOGGLED late — from PASS (flat line, day-old conviction) to SMALL LEAN FOR only when late money tightened onto france (+112 -> +100) in the final ~90m. this late-upgrade-driven-by-late-money toggle was CORRECT here (france covered). track whether "late money toggles the lean" proves a reliable signal over future entries, or whether it's noise / recency bias.

34fadestorm
wrong
back_favorite,follow_line_movement,situational_angle,overconfidence,total_over
line movement: atlanta -11.5/-12.5: -11.5 +100 (1d), -11 -110 (7h), -12 -115 (4h), -12 -115 (90m), -12.5 -108 (30m); total: u169 -110 (1d), u168.5 -110 (7h), u169 -110 (4h), u169.5 -110 (90m), u170 -110 (30m)
context: fade storm = back atlanta, the home favorite, on a "bad storm gets smashed by a good team in a low-scoring road blowout" read. the spread firmed one-directionally onto atlanta all day (-11.5 -> -12.5), a clean follow_line_movement signal that agreed with the fundamental, so the model took a MODERATE LEAN FOR atlanta -12.5; the total drifted UP (u169 -> u170), money leaning the over, so the model faded the "low-scoring" leg and (correctly) refused the under. atlanta won 89-78 but only by 11, so atlanta -12.5 did NOT cover — the fade was WRONG on the side. the total finished 167, UNDER 170, so the over the line movement was drifting toward also would have LOST. lesson: two lessons. first, backing a big WNBA favorite laying a huge number (-12.5) is exactly the overconfident "should get smashed" read that keeps underperforming (a hot underdog hung around and covered late), and following the line ONTO a favorite at an inflated key number is not the same edge as following it onto a value side. second and specifically: be cautious following a point-total OVER, especially in the WNBA. the line movement pointed at the over, but the game landed well UNDER — WNBA endgames routinely strand an over because the last few minutes can produce almost no points (intentional fouling, deliberate clock-milking, fewer possessions, garbage-time bench lineups in a blowout). the takeaway is to be CAUTIOUS with overs, not that line drift is meaningless: when forced to choose between a side and a total lean on the same game, lean toward the side and be willing to drop a shaky over.
model_lean: side (atlanta -12.5) — MODERATE LEAN FOR, MISS (won by only 11, didn't cover); total (over, via "fade the under") — mild lean toward the over but ultimately PASS, MISS-avoided (167 under 170, the over would have lost — the model was right to pass); parlay — advised AVOID, correctly avoided. net: the side lean MISSED (over-trusted a one-directional move onto an inflated WNBA favorite number), but the discipline to PASS the over rather than chase the drift saved a second loss — reinforcing the new caution-on-overs note.

35fadeaces
right
fade_favorite,fade_line_movement,situational_angle,overconfidence,total_over
line movement: aces spread: -9 -110 (1d), -9 -110 (9h), -9 -110 (6h), -9 -110 (3h), -9 -110 (2h), -10 -105 (1h), -10 -110 (30m), -10 -103 (5m), -9.5 -109 (1m); total: o176 -105 (1d), o175 -105 (9h), o175 -105 (6h), o174.5 -110 (3h), o174 -105 (2h), o174.5 -105 (1h), o175.5 -115 (30m), o175.5 -115 (5m), o175.5 -115 (1m)
context: fade the aces = back the home dog to keep it close, on a "too many points for an injured, unmotivated aces team laying -9/-10 as road favorites; should be a close, high-scoring game" read. the spread sat flat at -9 for a full day, then firmed onto the aces (-9 -> -10) in the last hour before a tiny buzzer giveback to -9.5. the aces won 88-80 but only by 8, so they did NOT cover -9.5/-10 — the dog covered and fading the aces was RIGHT on the side. the total, however, finished 168, well UNDER the ~o175.5 close, so the "high scoring" over leg was WRONG. lesson: the injured/unmotivated angle here was a REAL class/health situation, not the pure variance-dependent "keep it close" hope entry 33 warned about — a compromised favorite laying a big number is a legitimate fade, and it beat a late line move that had drifted onto the aces. also another WNBA over that died: the high-scoring read lost even though the side won, reinforcing the caution-on-overs note (take the side, drop the over).
model_lean: side (fade aces / back the dog) — the model LEANED AGAINST the fade (PASS/lean WITH the line onto aces -10, trusting the late move), MISS: the dog covered, so siding with the line was wrong here — a fundamental health/class angle beat the line, the recurring shape of the model's misses; total (over) — PASS with a mild over tilt, correctly PASSED and that mild over lean would have MISSED (168 under). net: MODEL MISS on the side — over-trusted a late, partly-reverting line move onto an injured favorite instead of respecting the legitimate class/health fade.

36fadefever
wrong
fade_favorite,fade_line_movement,situational_angle,overconfidence,total_under
line movement: phoenix spread: -1 -110 (9h), -1 -110 (6h), -1 -110 (3h), -.5 -105 (2h), -.5 -105 (1h), +1 -110 (30m), +1.5 -105 (15m), +2 -110 (5m); total: o170.5 -115 (9h), o170.5 -115 (6h), o170.5 -115 (3h), o171 -115 (2h), o171.5 -115 (1h), o171.5 -115 (30m), o171.5 -115 (15m), o171.5 -115 (5m)
context: fade the fever = back phoenix (the "better team") to blow out a tired fever on a b2b road game in a low-scoring game. but the line moved hard and continuously AGAINST phoenix: phoenix went from a -1 home favorite (ML -116) all the way to a +2 home dog by tip — a ~3-point one-directional move ONTO the fever — while the total ticked up (o170.5 -> o171.5). fever won outright 92-89 (by 3), so backing phoenix was WRONG on the side; the total finished 181, well OVER ~171.5, so the "low-scoring blowout" under thesis was also WRONG. lesson: textbook narrative-fights-line loss — a confident "should blow them out" read on a team the market was steadily and heavily fading (phoenix -1 -> +2) ignored the strongest signal in the log; trusting the sustained line move onto the fever would have been right on BOTH markets. the fever off-a-loss/b2b situational angle was already priced through and then some.
model_lean: side (fade fever / back phoenix) — the model STRONG-LEANED AGAINST the fade (toward the fever, with the ~3-pt line move), HIT: fever won; total (under) — the model leaned mild OVER against the low-scoring thesis, HIT direction (181, over); net: MODEL HIT — sided with a strong, sustained line move onto the fever against the phoenix-blowout narrative, and both the side and the total landed WITH the line (the narrative-fights-line + trust-the-line = model-hit pattern).

# Model Cache

Signal right/wrong record (based on tags):
follow_line_movement: 4 right / 4 wrong
resisted_live_doubledown: 2 right / 0 wrong
nervous_underdog_backing: 2 right / 0 wrong
fade_favorite: 7 right / 9 wrong
faded_tipster: 2 right / 1 wrong
vibes_over_logic: 1 right / 0 wrong
abandoned_winning_method: 1 right / 0 wrong
fresh_off_win: 1 right / 0 wrong
avoided_payout_chase: 1 right / 0 wrong
extras_risk: 2 right / 2 wrong
follow_consensus: 3 right / 2 wrong
prefer_simple_line: 1 right / 2 wrong
spread_nervousness: 2 right / 4 wrong
fade_consensus: 1 right / 3 wrong
situational_angle: 9 right / 15 wrong
decision_day_before: 3 right / 5 wrong
fade_line_movement: 7 right / 12 wrong
back_favorite: 5 right / 11 wrong
chased_better_payout: 0 right / 10 wrong
followed_tipster: 0 right / 6 wrong
missed_hedge: 0 right / 2 wrong
parlay_conflict: 0 right / 2 wrong
nervous_winner: 0 right / 1 wrong
line_stable: 0 right / 3 wrong
failure_to_cash_out: 0 right / 1 wrong
changed_mind: 0 right / 1 wrong
outlier_price: 0 right / 1 wrong
envy_driven: 0 right / 1 wrong
live_loss_spiral: 0 right / 1 wrong
fear_of_numbers: 0 right / 1 wrong
gamblers_fallacy: 1 right / 1 wrong
tilt_bet: 0 right / 1 wrong
price_deterioration: 0 right / 4 wrong
greed_driven: 0 right / 1 wrong
overcaution: 0 right / 1 wrong
misread_line_movement: 0 right / 1 wrong
spread_confidence: 0 right / 1 wrong
motivated_underdog: 1 right / 1 wrong
overconfidence: 5 right / 9 wrong
total_over: 5 right / 7 wrong
total_under: 1 right / 2 wrong

# Upcoming Events

fadebelgium
fade belgium off big time win against usa while spain off bad showing in win against portugal. spain by a million and belgium u0.5 should definitely hit. 4 days out, -.5 -164 is pricey but should hit super easy, total is o2.5 -121. 3 days out, the favorite is -.5 -160 and the total is o2.5 -120. 2 days out, the favorite is -.5 -152 and the total is o2.5 -120. 1 day out, the favorite is -.5 -154 and the total is o2.5 -124. 12 hours out, the favorite is -.5 -155 and the total is o2.5 -124 and there is a parlay to consider for better odds since spain has not allowed a goal this tournament and belgium should have an off day (spain -.5 with belgium u1.5 goals for -139) and another parlay (yamal o.5 shots_on_goal -501 & yamal o2.5 shots -257 & spain to advance  -325 to bring it all together for -117). 2 hours out, the favorite is -.5 -164 and the total is o2.5 -126 and there is a parlay to consider for better odds since spain has not allowed a goal this tournament and belgium should have an off day (spain -.5 with belgium u1.5 goals for -141) and another parlay (yamal o.5 shots_on_goal -667 & yamal o2.5 shots -385 & spain to advance -325 to bring it all together for -154). trent is on spain -.5 and most cappers are on spain either -.5 or to advance. 30 minutes, the favorite is -.5 -155 and the total is o2.5 -136.
lean (30m out):
<ins>side (spain -.5): PASS / small FADE-lean — still don't back it</ins> — the 2h spike to -164 REVERTED to -155, confirming it was noise, not fresh money onto spain. across the whole window the price has bounced in a -152/-164 range and now sits -155, net slightly OFF the -164 open = effectively line_stable (0/3) with a mild drift off the favorite. the underlying read is unchanged and still weak: overconfident "definitely hit" locked 4 days out (overconfidence 5/9, decision_day_before 3/5), trent on spain -.5 (followed_tipster 0/6), broad capper consensus on spain (follow_consensus 3/2, mild). a losing tipster signal on an already-overconfident, flat-lined favorite = the clean-fade shape. do not lay -155 on spain.
<ins>total (o2.5): SMALL LEAN FOR THE OVER (the one market with a real signal)</ins> — the over juice has climbed steadily and is accelerating (-121 -> -126 -> -136), one-directional money ONTO the over = follow_line_movement. unlike the WNBA caution-on-overs spots, this is a soccer o2.5 with a genuine line signal behind it. keep it SMALL — you're already paying -136 and total_over is 5/7 — but this is the cleanest edge on the ticket.
<ins>parlays: AVOID BOTH</ins> — unchanged: both are chased_better_payout (0/10). the spain -.5 + belgium u1.5 "-141 for better odds" adds a leg to dress up a price; the yamal SOG/shots + advance stack already deteriorated (-117 -> -154 = price_deterioration 0/4).
net: PASS the side (small fade of the overconfident spain read), a SMALL follow-the-line lean on the OVER, and hard-AVOID both parlays.

fadewings
way too many points for road favorite against a motivated home underdog off a loss to a good team. should be close high scoring game. 2 days out the favorite is -7 -110 and the total is o180.5 -110. 1 day out the favorite is -6.5 -110 and the total is o180 -110. 6 hours out the favorite is -8 -105 and the total is o178.5 -103.
lean (6h out):
<ins>side: PASS — the fade signal REVERSED (downgraded from the 1d moderate lean)</ins> — the confirming move toward the dog (-7 -> -6.5) round-tripped and then some: the spread firmed BACK OUT to -8 (money re-loading the ROAD FAVORITE), which is exactly the DENY condition I flagged. net from open the line is now ONTO the favorite (-7 -> -8), against the home dog, so the "motivated home dog keeps it close" fade no longer has line support. following the line now points at the road favorite -8, but that's a big road number + back_favorite (5/11), so no bet there either.
<ins>total (o178.5): lean UNDER / PASS the over</ins> — total dropped steadily (o180.5 -> o180 -> o178.5), against the "high scoring" read; caution-on-overs applies.
net: the one promising fade on the slate lost its edge when the line reversed onto the favorite. PASS both markets (mild under tilt).

fadesun
sun got happy playing injured teams recently so should get smashed as big home underdog in low scoring blow out. 2 days out, the home underdog is +9.5 -110 and the total is o154.5 -110. 1 day out, the home underdog is +7.5 -105 and the total is o153.5 -110. 6 hours out, the home underdog is +5.5 -115 and the total is o155.5 -107.
lean (6h out):
<ins>side (fade sun = back the road favorite): PASS / STRONGER LEAN AGAINST THE FADE</ins> — the DENY condition has fully triggered: the sun's number keeps shrinking (+9.5 -> +7.5 -> +5.5), ~4 points of money piling ONTO the home dog and off the favorite over the window. the "sun gets smashed / low-scoring blowout" read is the variance-dependent overconfident angle entries 33/34 flagged, and the market is emphatically on the SUN, not the favorite. follow_line_movement points at the sun; back_favorite is 5/11. do not back the favorite here.
<ins>total (o155.5): weak / PASS</ins> — o154.5 -> o153.5 -> o155.5 round-tripped back up, no clean signal and the "low scoring" leg is unsupported.
net: PASS; if anything the line says back the sun (the dog), the opposite of the fade.

fadesky
sparks got good win at home, should be motivated to continue against bad sky team who are small road underdog. expecting sparks to win by a lot in high scoring game. 2 days out, the home favorite is -1.5 -110 and the total is o181 -110. 1 day out, the home favorite is -1 -110 and the total is o179 -105. 9 hours out, the home favorite is -1.5 -106 and the total is o177 -105.
lean (9h out):
<ins>side (fade sky = back sparks to win by a lot): PASS / LEAN AGAINST THE "WIN BY A LOT" READ</ins> — the spread round-tripped (-1.5 -> -1 -> back to -1.5), net FLAT from open = line_stable (0/3), and it is still only -1.5. the market flatly refuses to price the blowout the thesis expects; my CONFIRM was a firm to -2/-3, which did NOT happen. "win by a lot" on a ~pick'em number is overconfidence (5/9) with no line support. at most a tiny sparks moneyline; never a big-margin bet.
<ins>total (o177): lean UNDER / PASS the over</ins> — o181 -> o179 -> o177 dropping steadily, against the "high scoring" read; caution-on-overs applies.
net: PASS; the flat, tiny spread and the steadily falling total both contradict a high-scoring sparks blowout.