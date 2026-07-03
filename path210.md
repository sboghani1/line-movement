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

4. Maintain the '# Model Cache' section: for each factor keep a running right-vs-wrong record (counts). Track totals only, not streaks or consecutive patterns.

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



first play. do not actually think this underdog is good, but now would be classic time to see favorite go down even if betonline is showing slight favor for the favorite. 
if trent is on egypt to advance -155ish then we have to take australia.



fadeliberty
like the road favorite against home hangover spot. but surprised they are favored outright, so nervous about points. subjectively think both sides will be available