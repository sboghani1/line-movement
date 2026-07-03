# Notes For Model

Purpose: This file is a decision log used to compare a decision currently under consideration against past outcomes, so we can learn which factors drive right vs. wrong binary decisions. Every decision resolves later to "right" or "wrong".

1. The 'Processes' section contains content split by 2 line breaks. Each chunk follows this schema:
name: string id starting with a number
result: either "right" or "wrong"
human_tags: comma-separated line of strings from human
model_tags: comma-separated line of strings from model
context: string starting with "context" which is explanation of why the decision was made. It should capture the setup/reasoning for the decision, what moved (line/price), the action taken, the result, and the retrospective lesson (the "should have...").

2. model_tags: read each entry's content and generate tags that make sense given all the other contexts. Prefer a consistent, reusable vocabulary so the same factor uses the same tag across entries (this is what makes patterns accumulate). Rules for generating/regenerating model_tags:
- Regenerate across the whole set, not just the new entry. When a new decision is logged, re-derive model_tags for ALL entries together so the vocabulary stays normalized.
- Rebuild the '# Model Cache' after any (re)generation, since its counts are derived from model_tags and must stay in sync.
- Only ever edit model_tags. Never modify name, result, human_tags, or context — those are the source of truth.
- Tag format/quality: lowercase snake_case; each tag must be a reusable factor that could recur in a future decision (not a one-off description); never encode the outcome as a tag (no "wrong"/"loss"); consolidate near-duplicates into one canonical tag.
- Derive tags from context (using human_tags as hints), independent of result, so tags describe the decision's factors rather than hindsight.

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
decision_day_before
fade_favorite,fade_line_movement,decision_day_before,failure_to_cash_out,followed_tipster,changed_mind
context: first play. wanted to fade favorite from day before because dubundo on it. morning of game: line movement towards favorite, took fade. movement in our favor. trent backs us. trent was off a good day, wanted to fade his first bet, ended up tailing it.  lost. should have cashed out when trent backed us.

2fadebtts
wrong
against_the_world,hedge_opportunity
fade_consensus,outlier_price,missed_hedge
context: second play. wanted to fade absolute consensus on btts. betonline was so much higher than everyone -200 vs -130. first half no goals. second half both sides score.

3fadeperfectmlb
wrong
envy_driven_fade,instant_loser,infinite_live_losses
fade_consensus,fade_line_movement,envy_driven,chased_better_payout,live_loss_spiral,fear_of_numbers
third play. wanted to fade reisch on 4-0 day he had o8.5, he was on consensus & line movement side of mlb. i was staring at u9.5, it came to u9 better price so i jumped. instant loser to 11.5 live, 13.5, 17.5, 18.5, 19.5, 20.5, ended on 19. i was too scared of every number and thought 18.5 was the trap.

4fadelastfavorite
wrong
square_conviction_driven
fade_favorite,gamblers_fallacy,tilt_bet,chased_better_payout,parlay_conflict
third play. previous day all favorites won, two close. convinced today a big upset was due. lost first two underdog bets. rage bet underdog in regulation instead of double chance because of amazing return. this was also third leg for kuku favorite parlay. underdog loses by 2.

5fadeengland
right
nervous_underdog_backing
fade_favorite,fade_consensus,follow_line_movement,resisted_live_doubledown
first play. consistent movement away from square position -2. wanted to wait for live better line, but took +1.5. dog takes early lead, wanted to live on -.5. favorite comes back to win by 1, so we win. but almost got tricked into live loss.

6fadeebelgium
right
nervous_underdog_backing,vibes_only
fade_favorite,vibes_over_logic,abandoned_winning_method,fresh_off_win,resisted_live_doubledown
second play. freshly off a win based on logic. same logic was pointing us to the favorite this time, but decided it made sense to go against the previous wininng method. almost took live double down when underdog was winning, but favorite tied at the very end then won in extra. we would have lost every live try.

# Model Cache

Signal right/wrong record (based on model_tags):
resisted_live_doubledown: 2 right / 0 wrong
follow_line_movement: 1 right / 0 wrong
vibes_over_logic: 1 right / 0 wrong
abandoned_winning_method: 1 right / 0 wrong
fresh_off_win: 1 right / 0 wrong
fade_favorite: 2 right / 2 wrong
fade_consensus: 1 right / 2 wrong
fade_line_movement: 0 right / 2 wrong
chased_better_payout: 0 right / 2 wrong
decision_day_before: 0 right / 1 wrong
failure_to_cash_out: 0 right / 1 wrong
followed_tipster: 0 right / 1 wrong
changed_mind: 0 right / 1 wrong
outlier_price: 0 right / 1 wrong
missed_hedge: 0 right / 1 wrong
envy_driven: 0 right / 1 wrong
live_loss_spiral: 0 right / 1 wrong
fear_of_numbers: 0 right / 1 wrong
gamblers_fallacy: 0 right / 1 wrong
tilt_bet: 0 right / 1 wrong
parlay_conflict: 0 right / 1 wrong

# Notes to ignore 
fadeegypt
first play. do not actually think this underdog is good, but now would be classic time to see favorite go down even if betonline is showing slight favor for the favorite. 
candidate_tags: fade_favorite, gamblers_fallacy, fade_line_movement, no_conviction
lean: negative — lean toward NOT making this bet.
strength: strong
reasoning: stacks two loss-only factors (gamblers_fallacy 0/1, fade_line_movement 0/2) with explicitly zero conviction in the pick. nearest analog is entry 4 (4fadelastfavorite, "upset due" fade of favorites) which lost. both winning entries (5, 6) went the opposite way: they followed line movement / let the favorite come through and resisted live temptation (follow_line_movement 1/0, resisted_live_doubledown 2/0). here we'd be fading the side the line favors, with no read — the wrong-correlated pattern. only mitigating factor fade_favorite is a coin flip (2/2), not enough to offset. note: no_conviction is a new reusable factor with no history yet.
fadeeypyt_cont
if trent is on egypt to advance -155ish then we have to take australia. but he is on btts expecting a draw. egypt in regulation is +138 which feels not too trappy but a little intimidating to take. seems like general cappers like the favorite to win but not cover.
final_lean:
tags: back_favorite, follow_consensus, chased_better_payout, prefer_simple_line, extras_risk, spread_nervousness
direction: positive on Egypt to advance (~-155), negative on Egypt regulation +138.
strength: moderate.
reasoning: today's input resolves yesterday's concerns — this is now backing the favorite WITH consensus ("cappers like favorite to win"), not fading it with no conviction, so fade_favorite/gamblers_fallacy/fade_line_movement no longer apply. the trent "fade → take australia" trigger does not fire, since he's on btts/draw, not egypt-to-advance. the real choice is line selection: egypt-to-advance -155 (simple) vs egypt-regulation +138 (more payout). taking the +138 regulation is chased_better_payout (0/2, our worst-correlated factor) and directly exposed to the extras_risk that the consensus itself flags — "wins but not cover" means egypt can win in extra time and the regulation bet still loses. prefer_simple_line says take the safe -155 advance. only wrong if egypt is eliminated outright, which no read supports.
result: got greedy and took moneyline +140. favorite went up and halftime hedge for half was available. did not take, underdog came back. at this point moneyline is +200, +300 all the way to +600. ss is on australia-to-advance. i take second bet on egypt-to-advance at -143. it never goes worse, gets better all the way to -118, regulation ends on -133 to -143. egypt advances in penalties. two major learnings should be about (avoiding -143 when -118 might come) and (take halftime hedge for 50% guarantee, be careful if more than 50% is available for soccer maybe, need more data)

fadecapeverde
argentina should easily win 2-0 or 3-0, but considering parlay nbtts and o1.5 total for +100. simpler would be -2 -126 (day before).
tags: back_favorite, chased_better_payout, parlay_conflict, decision_day_before, prefer_simple_line
lean: take simple -2, avoid the +100 parlay — parlay = chased_better_payout (0/2); also day-before, so ideally wait. strength: strong (against parlay)
fadecapeverde_cont
5 hours away, the -2 is at -128, so not any real movement. everyone has argentina parlayed with egypt to advance and colombia to advance. 1 hour away, -2 is at -136. egypt advanced. trent is off a win and now on the total under 2.5 +130. it is +143 on betonline. 
final_lean:
tags: back_favorite, follow_consensus, follow_line_movement, chased_better_payout, prefer_simple_line, price_deterioration, spread_nervousness, followed_tipster
direction: positive on Argentina to win outright (moneyline); cautious/neutral on -2 at -136; negative on the +100 parlay; negative on Trent's under 2.5 +130.
strength: moderate.
reasoning: Argentina winning is the high-probability consensus read and the -2 drift (-128 -> -136) is follow_line_movement (1/0 win signal), so the direction is sound. but two frictions cap conviction: (1) price_deterioration — the number went from -126 to -136, and the egypt lesson was "avoid the worse price when a better one existed"; paying up with the public is a worse entry. (2) spread_nervousness — the stated outcome is "2-0 or 3-0," and a 2-0 result PUSHES the -2, so the spread carries real not-cover/push risk, the same trap flagged on egypt. cleanest expression is therefore Argentina moneyline rather than -2, or pass if only -2/parlay are offered. the +100 parlay is chased_better_payout (0/2, worst factor). Trent's under 2.5 +130 is a followed_tipster play (0/1) that also contradicts the blowout thesis (3-0 goes over), so avoid it.

fadeghana
insane home crowd prob. colombia -1.5 is +132 day before. u2.5 -127 feels like the nervous winner, colombia wins 2-0 late type of game.
tags: back_favorite, situational_angle, decision_day_before, nervous_winner
lean: lean u2.5 (safer), avoid forcing -1.5 +132 — the "nervous winner" under is the cleaner read; caution on day-before timing. strength: weak
9 hours away the -1.5 is still +132. 3 hours away the -1.5 is still +132.

enumerated_leans (each an independent read from all data except the other leans here):
baseline market facts: colombia is the favorite; own read is "colombia wins 2-0 late" nervous_winner; -1.5 is +132 and line_stable (unmoved 9h & 3h out, so no fade_line_movement risk either way); insane ghana home crowd = situational_angle for a close/live game. tipster overlay: followed_tipster is 0/1 in cache, and entry 1 shows tailing/fading trent around a good-then-bad swing lost. so trent's pick is a weak positive when it agrees with an independent read, and NOT a reason to override one.

--- A1: trent hot, hit the under, off back-to-back wins ---
A1+B1 colombia -1.5: tags back_favorite, follow_consensus, spread_nervousness, followed_tipster, line_stable. lean POSITIVE but capped, strength weak-moderate. reasoning: agrees with favorite thesis and a hot tipster, but own read is "2-0 late" which pushes/loses -1.5 more often than the +132 implies; nervous_winner sits on the total, not the spread.
A1+B2 ghana +1.5: tags fade_favorite, situational_angle, contrarian. lean NEGATIVE, strength weak-moderate. reasoning: taking the home dog to cover cuts against both the favorite read and a hot tipster who's on the other side; home crowd is the only support.
A1+B3a over 2.5: tags total_over, contrarian_to_read. lean NEGATIVE, strength weak. reasoning: over needs 3+ goals; own "2-0 late" read is a push-to-under, and a hot trent on the under reinforces staying away from the over.
A1+B3b under 2.5: tags total_under, nervous_winner, follow_consensus, followed_tipster. lean POSITIVE, strength moderate. reasoning: this is the cleanest expression of the original read AND aligns with a hot tipster; best branch of the group.
A1+B4a btts yes: tags btts, situational_angle. lean NEUTRAL-slightly-negative, strength weak. reasoning: ghana home crowd can grab one, but "colombia 2-0 clean-ish" read leans no; hot trent adds nothing here.
A1+B4b btts no: tags btts, back_favorite, follow_consensus. lean POSITIVE, strength weak-moderate. reasoning: colombia clean sheet fits the 2-0 read; correlated with under, modest edge.

--- A2: argentina won by 3+, trent's under lost, trent off a win-then-loss start ---
A2+B1 colombia -1.5: tags back_favorite, spread_nervousness, followed_tipster, tipster_bounceback_risk. lean NEUTRAL, strength weak. reasoning: same push/loss spread risk as A1+B1, and now trent is off a loss — entry 1's win-then-swing tipster pattern is our loss analog, so his pick carries less weight; don't chase the spread.
A2+B2 ghana +1.5: tags fade_favorite, situational_angle, contrarian, tipster_bounceback_risk. lean NEGATIVE, strength weak-moderate. reasoning: still against the favorite read; a cold trent chasing a bounce-back dog is the entry-1 tilt shape, avoid.
A2+B3a over 2.5: tags total_over, contrarian_to_read, tipster_bounceback_risk. lean NEGATIVE, strength weak-moderate. reasoning: over contradicts the 2-0 read; a cold tipster reaching for a plus-money over is exactly the chased_better_payout/tilt trap.
A2+B3b under 2.5: tags total_under, nervous_winner, tipster_bounceback_risk. lean POSITIVE but slightly softer than A1+B3b, strength weak-moderate. reasoning: independent read still likes the under, but trent being cold removes the tailing tailwind, so trim conviction vs A1.
A2+B4a btts yes: tags btts, situational_angle, tipster_bounceback_risk. lean NEUTRAL, strength weak. reasoning: home crowd angle only; cold-tipster overlay adds no edge.
A2+B4b btts no: tags btts, back_favorite. lean POSITIVE, strength weak. reasoning: fits the clean-sheet 2-0 read on its own merits; trent's state is not a factor since he's not the driver here.