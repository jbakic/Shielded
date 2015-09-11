using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Shielded;
using Shielded.ProxyGen;
using System.Diagnostics;

namespace ConsoleTests
{
    public class Event
    {
        public virtual int Id { get; set; }
        public virtual string HomeTeam { get; set; }
        public virtual string AwayTeam { get; set; }
        public readonly IList<BetOffer> BetOffers =
            new ShieldedSeq<BetOffer>();
    }

    public class BetOffer
    {
        public virtual int Id { get; set; }
        public virtual Event Event { get; set; }
        public virtual string Pick { get; set; }
        public virtual decimal Odds { get; set; }
    }

    public class Ticket
    {
        public virtual int Id { get; set; }
        public virtual decimal PayInAmount { get; set; }
        public virtual decimal WinAmount { get; set; }
        public virtual Bet[] Bets { get; set; }
    }

    public class Bet
    {
        public virtual BetOffer Offer { get; set; }
        public virtual decimal Odds { get; set; }
    }

    public class BetShop
    {
        public readonly ShieldedDict<int, Event> Events;

        private int _ticketIdGenerator = 0;
        public readonly ShieldedDict<int, Ticket> Tickets =
            new ShieldedDict<int, Ticket>();
        private ShieldedDictNc<string, decimal> _sameTicketWins =
            new ShieldedDictNc<string, decimal>();

        public decimal SameTicketWinLimit = 1000m;

        static string GetOfferHash(Ticket newTicket)
        {
            return string.Join(",", newTicket.Bets
                .Select(b => b.Offer.Id).OrderBy(id => id));
        }

        public Ticket BuyTicket(decimal payIn, params BetOffer[] bets)
        {
            var newId = Interlocked.Increment(ref _ticketIdGenerator);
            var newTicket = Factory.NewShielded<Ticket>();
            return Shield.InTransaction(() =>
            {
                newTicket.Id = newId;
                newTicket.PayInAmount = payIn;
                newTicket.Bets = bets.Select(shBo => {
                    var bet = Factory.NewShielded<Bet>();
                    bet.Offer = shBo;
                    bet.Odds = shBo.Odds;
                    return bet;
                }).ToArray();
                newTicket.WinAmount = newTicket.PayInAmount *
                    newTicket.Bets.Aggregate(1m, (curr, nextBet) => curr * nextBet.Odds);

                var hash = GetOfferHash(newTicket);
                var totalWin = _sameTicketWins.ContainsKey(hash) ?
                    _sameTicketWins[hash] + newTicket.WinAmount : newTicket.WinAmount;
                if (totalWin > SameTicketWinLimit)
                    return null;

                Tickets[newId] = newTicket;
                _sameTicketWins[hash] = totalWin;
                return newTicket;
            });
        }

        private void PrepareFactory()
        {
            Stopwatch sw = Stopwatch.StartNew();
            Factory.PrepareTypes(new [] {
                typeof(Event),
                typeof(BetOffer),
                typeof(Ticket),
                typeof(Bet)
            });
            sw.Stop();
            Console.WriteLine("Factory prepared proxies in {0} ms.", sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// Creates n events, with three typical offers (1,X,2) for each.
        /// The events get IDs 1-n.
        /// </summary>
        public BetShop(int n)
        {
            PrepareFactory();

            List<Event> initialEvents = new List<Event>();

            Shield.InTransaction(() =>
            {
                int eventIdGenerator = 1;
                int offerIdGenerator = 1;
                for (int i = 0; i < n; i++)
                {
                    var newEvent = Factory.NewShielded<Event>();
                    newEvent.Id = eventIdGenerator++;
                    newEvent.HomeTeam = "Home " + i;
                    newEvent.AwayTeam = "Away " + i;
                    initialEvents.Add(newEvent);

                    var no = Factory.NewShielded<BetOffer>();
                    no.Id = offerIdGenerator++;
                    no.Event = newEvent;
                    no.Pick = "1";
                    no.Odds = 2m;
                    newEvent.BetOffers.Add(no);

                    no = Factory.NewShielded<BetOffer>();
                    no.Id = offerIdGenerator++;
                    no.Event = newEvent;
                    no.Pick = "X";
                    no.Odds = 4m;
                    newEvent.BetOffers.Add(no);

                    no = Factory.NewShielded<BetOffer>();
                    no.Id = offerIdGenerator++;
                    no.Event = newEvent;
                    no.Pick = "2";
                    no.Odds = 4.5m;
                    newEvent.BetOffers.Add(no);
                }
            });

            Events = new ShieldedDict<int, Event>(
                initialEvents.Select(e => new KeyValuePair<int, Event>(e.Id, e)).ToArray());
        }

        /// <summary>
        /// Checks if the rule about limiting same tickets was violated.
        /// </summary>
        public bool VerifyTickets()
        {
            return Shield.InTransaction(() =>
            {
                Dictionary<string, decimal> checkTable = new Dictionary<string, decimal>();
                var count = 0;
                foreach (var t in Tickets.Values)
                {
                    var hash = GetOfferHash(t);
                    if (!checkTable.ContainsKey(hash))
                        checkTable[hash] = t.WinAmount;
                    else
                        checkTable[hash] = checkTable[hash] + t.WinAmount;
                    if (checkTable[hash] > SameTicketWinLimit)
                        return false;
                    count++;
                }
                if (Tickets.Count != count)
                    throw new ApplicationException("Wrong Count!");
                return true;
            });
        }
    }
}
