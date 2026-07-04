# Notes For Model

Purpose: This file is a decision log used to compare a decision currently under consideration against past outcomes, so we can learn which factors drive right vs. wrong binary decisions. Every decision resolves later to "right" or "wrong".

1. The 'Processes' section contains content split by 2 line breaks. Each chunk follows this schema:
name: string id starting with a number
result: either "right" or "wrong"
tags: single comma-separated line of strings (human-provided and model-generated tags are merged here, deduplicated into one normalized set)
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

4. Maintain the '# Model Cache' section: for each factor keep a running right-vs-wrong record (counts). Track totals only, not streaks or consecutive patterns.

5. Continuation entries: a pending decision in '# Notes to ignore' may be extended over time with one or more '<name>_cont' blocks appended after the prior lean, each holding new information (line moves, tipster positions, time-to-event, etc.). When asked to update a lean, read the original entry plus ALL its '<name>_cont' blocks together, then write a fresh block at the bottom (e.g. 'updated_lean:' / 'final_lean:') with refreshed tags, direction, strength, and reasoning. Do not edit the earlier leans — append, so the evolution of the read is preserved.

# Processes

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
right
back_favorite,follow_consensus,nervous_winner,total_under,prefer_simple_line,faded_tipster,line_stable,situational_angle
context: colombia favorite over ghana (insane home crowd), -1.5 at +132 and stable 9h/3h out. read was "colombia wins 2-0 late" nervous_winner, so leaned the u2.5 as the cleaner play and avoided forcing the -1.5. trent was off 2 wins and on colombia -1.5. colombia won 1-0: the under hit (1 goal), the -1.5 did not cover (margin 1) so avoiding the spread was correct, and trent's -1.5 lost. nervous_winner read landed exactly.

10fadeliberty
wrong
back_favorite,situational_angle,spread_nervousness,overcaution,misread_line_movement
context: road favorite against a home-hangover spot; liked the angle but got nervous about the points and talked myself off it, reading the small -2.5 -> -2 drift as a fade_line_movement warning. liberty won by 13, so the spread cashed easily. the caution was the error: a minor line wiggle in a class mismatch was overweighted, and the sound situational read got passed. lesson: a tiny drift off a favorite is not a real fade signal; don't let points-nervousness kill a good angle. also having strong conviction day in advance without change in line movement is a sign for fade.

11fadesky
wrong
back_favorite,situational_angle,spread_confidence,fade_line_movement,motivated_underdog
context: backed chicago as the home favorite in a 'maybe motivated' spot, trusting the class gap and dismissing points fear. read -7 as stable 9h out, but the line drifted to -4 by close — the market was fading chicago. chicago lost by 8 outright. the closing drift (-7 -> -4) was the fade_line_movement warning i overlooked, and the 'maybe motivated' underdog risk materialized. lesson: track the CLOSING line, not just an early snapshot; an adverse drift off your favorite plus a live motivation angle is a real loss signal, not to be overridden by spread_confidence.

# Model Cache

Signal right/wrong record (based on tags):
follow_line_movement: 2 right / 0 wrong
resisted_live_doubledown: 2 right / 0 wrong
nervous_underdog_backing: 2 right / 0 wrong
faded_tipster: 2 right / 0 wrong
vibes_over_logic: 1 right / 0 wrong
abandoned_winning_method: 1 right / 0 wrong
fresh_off_win: 1 right / 0 wrong
avoided_payout_chase: 1 right / 0 wrong
nervous_winner: 1 right / 0 wrong
total_under: 1 right / 0 wrong
line_stable: 1 right / 0 wrong
follow_consensus: 2 right / 1 wrong
prefer_simple_line: 2 right / 1 wrong
fade_favorite: 2 right / 2 wrong
back_favorite: 2 right / 3 wrong
extras_risk: 1 right / 1 wrong
spread_nervousness: 1 right / 1 wrong
fade_consensus: 1 right / 2 wrong
situational_angle: 1 right / 2 wrong
chased_better_payout: 0 right / 3 wrong
fade_line_movement: 0 right / 3 wrong
missed_hedge: 0 right / 2 wrong
decision_day_before: 0 right / 1 wrong
failure_to_cash_out: 0 right / 1 wrong
followed_tipster: 0 right / 1 wrong
changed_mind: 0 right / 1 wrong
outlier_price: 0 right / 1 wrong
envy_driven: 0 right / 1 wrong
live_loss_spiral: 0 right / 1 wrong
fear_of_numbers: 0 right / 1 wrong
gamblers_fallacy: 0 right / 1 wrong
tilt_bet: 0 right / 1 wrong
parlay_conflict: 0 right / 1 wrong
price_deterioration: 0 right / 1 wrong
greed_driven: 0 right / 1 wrong
overcaution: 0 right / 1 wrong
misread_line_movement: 0 right / 1 wrong
spread_confidence: 0 right / 1 wrong
motivated_underdog: 0 right / 1 wrong

# Notes to ignore 
