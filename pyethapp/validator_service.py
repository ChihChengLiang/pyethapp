from __future__ import print_function
import time
from enum import Enum
from devp2p.service import BaseService
from ethereum.slogging import get_logger
from ethereum.tools import tester
from ethereum import transactions, abi, utils
from ethereum.hybrid_casper import casper_utils

log = get_logger('eth.validator')


class ValidatorState(Enum):
    uninitiated = 1             # Check if logged in, and if not deploy a valcode contract
    waiting_for_valcode = 2     # Wait for valcode ct to be included, then submit deposit
    waiting_for_login = 3       # Wait for validator to login, then change state to `voting`
    voting = 4                  # Vote on each new epoch
    waiting_for_log_out = 5
    waiting_for_withdrawable = 6
    waiting_for_withdrawn = 7
    logged_out = 8


# TODO: Check if user is logged in & reinitialize based on contract

class ValidatorService(BaseService):

    name = 'validator'
    default_config = dict(validator=dict(
        deposit_size=0
    ))

    def __init__(self, app):
        super(ValidatorService, self).__init__(app)
        # Check if we should actually validate, if not return
        if not app.config['validate']:
            return
        log.info('Validator enabled!')

        self.config = app.config
        self.chainservice = app.services.chain
        self.chain = self.chainservice.chain
        self.deposit_size = self.config['deposit_size']
        self.should_logout = self.config['should_logout']
        self.valcode_addr = None
        self.epoch_length = self.chain.env.config['EPOCH_LENGTH']
        self.votes = dict()
        self.latest_target_epoch = -1
        self.latest_source_epoch = -1
        self.coinbase = app.services.accounts.find(app.config['validate'][0])
        # Set new block callback. This will trigger validation logic
        app.services.chain.on_new_head_cbs.append(self.on_new_head)
        # Set up the validator's state & handlers
        self.set_current_state(ValidatorState.uninitiated)
        self.logout_broadcast_cooldown = 60
        self.last_logout_broadcast = 0

        self.handlers = {
            ValidatorState.uninitiated: self.check_logged_in,
            ValidatorState.waiting_for_valcode: self.check_valcode,
            ValidatorState.waiting_for_login: self.check_logged_in,
            ValidatorState.voting: self.vote,
            ValidatorState.waiting_for_log_out: self.vote_then_logout,
            ValidatorState.waiting_for_withdrawable: self.check_withdrawable,
            ValidatorState.waiting_for_withdrawn: self.check_withdrawn,
            ValidatorState.logged_out: self.check_logged_in
        }

    def on_new_head(self, block):
        if self.app.services.chain.is_syncing:
            return
        casper = tester.ABIContract(tester.State(self.chain.state.ephemeral_clone()),
                                    casper_utils.casper_abi, self.chain.casper_address)
        self.log_casper_info(casper)
        self.handlers[self.current_state](casper)

    def check_logged_in(self, casper):
        validator_index = self.get_validator_index(casper)
        # (1) Check if the validator has ever deposited funds
        if not validator_index and self.deposit_size:
            log.debug('The validator hasn\'t deposited funds but deposit flag is set, so deposit!')
            self.broadcast_valcode_tx()
            self.set_current_state(ValidatorState.waiting_for_valcode)
        elif not validator_index:
            log.debug('The validator hasn\'t deposited funds and we have no intention to, so return!')
            return
        # (2) Check if the validator is logged in
        if not self.is_logged_in(casper, casper.current_epoch(), validator_index):
            log.debug('The validator is not logged in')
            return

        self.log_validator_info(casper)
        # The validator is logged in, check if we should start start voting or a logout sequence
        if self.should_logout:
            log.info('Changing validator state to log out')
            self.set_current_state(ValidatorState.waiting_for_log_out)
        else:
            log.info('Changing validator state to voting')
            self.set_current_state(ValidatorState.voting)

    def check_valcode(self, casper):
        if not self.chain.state.get_code(self.valcode_addr):
            log.debug('Valcode still not deployed!')
            return
        # Make sure we have enough ETH to deposit
        if self.chain.state.get_balance(self.coinbase.address) < self.deposit_size:
            log.info('Cannot login as validator: Not enough ETH!')
            return
        # Valcode deployed! Let's deposit
        self.broadcast_deposit_tx()
        self.set_current_state(ValidatorState.waiting_for_login)

    def vote_then_logout(self, casper):
        epoch = self.chain.state.block_number // self.epoch_length
        validator_index = self.get_validator_index(casper)
        # Verify that we are not already logged out
        if not self.is_logged_in(casper, epoch, validator_index):
            # If we logged out, start waiting for withdrawls
            log.info('Validator logged out!')
            self.set_current_state(ValidatorState.waiting_for_withdrawable)
            return None
        logout_tx_nonce = self.chain.state.get_nonce(self.coinbase.address)
        vote_successful = self.vote(casper)
        if vote_successful:
            logout_tx_nonce += 1
        self.broadcast_logout_tx(casper, logout_tx_nonce)
        self.set_current_state(ValidatorState.waiting_for_log_out)

    def vote(self, casper):
        log.info('Attempting to vote')
        epoch = self.chain.state.block_number // self.epoch_length
        # NO_DBL_VOTE: Don't vote if we have already
        if epoch in self.votes:
            log.debug('No vote: [NO_DBL_VOTE]', epoch=epoch, votes=self.votes.keys())
            return False
        validator_index = self.get_validator_index(casper)
        # Make sure we are logged in
        if not self.is_logged_in(casper, epoch, validator_index):
            raise Exception('Cannot vote: Validator not logged in!')
        if self.chain.state.block_number % self.epoch_length <= self.epoch_length / 4:
            log.debug('Wait until a quarter of epoch')
            return False
        # Get the ancestry hash and source ancestry hash
        target_hash, epoch, source_epoch = self.recommended_vote_contents(casper, validator_index)
        if target_hash is None:
            return False
        # Prevent NO_SURROUND slash
        if epoch < self.latest_target_epoch or source_epoch < self.latest_source_epoch:
            log.debug('No vote: [NO_SURROUND]')
            return False
        vote_msg = casper_utils.mk_vote(validator_index, target_hash, epoch,
                                        source_epoch, self.coinbase.privkey)
        # Save the vote message we generated
        self.votes[epoch] = vote_msg
        self.latest_target_epoch = epoch
        self.latest_source_epoch = source_epoch
        # Send the vote!
        vote_tx = self.mk_vote_tx(vote_msg)
        self.chainservice.broadcast_transaction(vote_tx)
        log.info('Sent vote! Tx: {}'.format(str(vote_tx)))
        log.info('Vote submitted: validator %d - epoch %d - source_epoch %d - hash %s' %
                 (self.get_validator_index(casper),
                  epoch, source_epoch, utils.encode_hex(target_hash)))
        return True

    def check_withdrawable(self, casper):
        vindex = self.get_validator_index(casper)
        if vindex == 0:
            log.info('Validator is already deleted!')
            self.set_current_state(ValidatorState.logged_out)
            return
        end_epoch = casper.dynasty_start_epoch(casper.validators__end_dynasty(vindex) + 1)
        # Check Casper to see if we can withdraw
        if casper.current_epoch() >= end_epoch + casper.withdrawal_delay():
            # Make withdraw tx & broadcast
            withdraw_tx = self.mk_withdraw_tx(self.get_validator_index(casper))
            self.chainservice.broadcast_transaction(withdraw_tx)
            # Set the state to waiting for withdrawn
            self.set_current_state(ValidatorState.waiting_for_withdrawn)

    def check_withdrawn(self, casper):
        # Check that we have been withdrawn--validator index will now be zero
        if casper.validator_indexes(self.coinbase.address) == 0:
            self.set_current_state(ValidatorState.logged_out)

    def log_casper_info(self, casper):
        ce = casper.current_epoch()
        ese = casper.expected_source_epoch()
        cur_deposits = casper.get_total_curdyn_deposits()
        prev_deposits = casper.get_total_prevdyn_deposits()
        cur_votes = casper.votes__cur_dyn_votes(ce, ese) * casper.deposit_scale_factor(ce)
        prev_votes = casper.votes__prev_dyn_votes(ce, ese) * casper.deposit_scale_factor(ce)
        cur_vote_pct = cur_votes * 100 / cur_deposits if cur_deposits else 0
        prev_vote_pct = prev_votes * 100 / prev_deposits if prev_deposits else 0
        last_finalized_epoch, last_justified_epoch = casper.last_finalized_epoch(), casper.last_justified_epoch()
        last_nonvoter_rescale, last_voter_rescale = casper.last_nonvoter_rescale(), casper.last_voter_rescale()
        log.info('CASPER STATUS: epoch %d, %.3f / %.3f ETH (%.2f %%) voted from current dynasty, '
                 '%.3f / %.3f ETH (%.2f %%) voted from previous dynasty, last finalized epoch %d justified %d '
                 'expected source %d. Nonvoter deposits last rescaled %.5fx, voter deposits %.5fx' %
                 (ce, cur_votes / 10**18, cur_deposits / 10**18, cur_vote_pct,
                  prev_votes / 10**18, prev_deposits / 10**18, prev_vote_pct,
                  last_finalized_epoch, last_justified_epoch, ese,
                  last_nonvoter_rescale, last_voter_rescale
                  ))

    def log_validator_info(self, casper):
        index = self.get_validator_index(casper)
        address = casper.validators__addr(index)
        deposit_unscaled = casper.validators__deposit(index)
        ce = casper.current_epoch()
        deposit_scaled = deposit_unscaled * casper.deposit_scale_factor(ce)
        start_dynasty = casper.validators__start_dynasty(index)
        end_dynasty = casper.validators__end_dynasty(index)
        withdrawal_addr = casper.validators__withdrawal_addr(index)
        log.info('Validator Status:', index=index, address=address,
            deposit_unscaled=deposit_unscaled / 10**18,
            deposit_scaled=deposit_scaled / 10**18,
            start_dynasty=start_dynasty,
            end_dynasty=end_dynasty,
            withdrawal_addr=withdrawal_addr)

    def set_current_state(self, validator_state):
        log.info('Changing validator state to: {}'.format(validator_state))
        self.current_state = validator_state

    def broadcast_valcode_tx(self):
        valcode_tx = self.mk_transaction('', 0,
                                         casper_utils.mk_validation_code(self.coinbase.address))
        nonce = self.chain.state.get_nonce(self.coinbase.address)
        self.valcode_addr = utils.mk_contract_address(self.coinbase.address, nonce)
        log.info('Broadcasting valcode tx with nonce: {}'.format(valcode_tx.nonce))
        self.chainservice.broadcast_transaction(valcode_tx)

    def broadcast_deposit_tx(self):
        # Create deposit transaction
        casper_ct = abi.ContractTranslator(casper_utils.casper_abi)
        deposit_func = casper_ct.encode('deposit', [self.valcode_addr, self.coinbase.address])
        deposit_tx = self.mk_transaction(self.chain.casper_address,
                                         self.deposit_size, deposit_func)
        # Broadcast it!
        log.info('Broadcasting deposit tx with nonce: {}'.format(deposit_tx.nonce))
        self.deposit_size = None
        self.chainservice.broadcast_transaction(deposit_tx)

    def broadcast_logout_tx(self, casper, nonce):
        if self.last_logout_broadcast > time.time() - self.logout_broadcast_cooldown:
            return
        self.last_logout_broadcast = time.time()
        epoch = self.chain.state.block_number // self.epoch_length
        # Generage the message
        logout_msg = casper_utils.mk_logout(self.get_validator_index(casper),
                                            epoch, self.coinbase.privkey)
        # Generate transactions
        logout_tx = self.mk_logout_tx(logout_msg, nonce)
        log.info('Logout Tx broadcasted: {}'.format(str(logout_tx)))
        self.chainservice.broadcast_transaction(logout_tx)

    def mk_transaction(self, to=b'\x00' * 20, value=0, data=b'',
                       gasprice=110*10**9, startgas=2500000, nonce=None, signed=True):
        if nonce is None:
            nonce = self.chain.state.get_nonce(self.coinbase.address)
        tx = transactions.Transaction(nonce, gasprice, startgas, to, value, data)
        if signed:
            self.coinbase.sign_tx(tx)
        return tx

    def is_logged_in(self, casper, target_epoch, validator_index):
        start_dynasty = casper.validators__start_dynasty(validator_index)
        end_dynasty = casper.validators__end_dynasty(validator_index)
        current_dynasty = casper.dynasty_in_epoch(target_epoch)
        past_dynasty = current_dynasty - 1
        in_current_dynasty = ((start_dynasty <= current_dynasty) and
                              (current_dynasty < end_dynasty))
        in_prev_dynasty = ((start_dynasty <= past_dynasty) and (past_dynasty < end_dynasty))
        if not (in_current_dynasty or in_prev_dynasty):
            return False
        return True

    def get_validator_index(self, casper):
        try:
            return casper.validator_indexes(self.coinbase.address)
        except tester.TransactionFailed:
            return None

    def recommended_vote_contents(self, casper, validator_index):
        current_epoch = casper.current_epoch()
        if current_epoch == 0:
            return None, None, None
        # NOTE: Using `epoch_blockhash` because currently calls to `blockhash` within contracts
        # in the ephemeral state are off by one, so we can't use `get_recommended_target_hash()` :(
        target_hash = self.epoch_blockhash(current_epoch)
        source_epoch = casper.recommended_source_epoch()
        return target_hash, current_epoch, source_epoch

    def epoch_blockhash(self, epoch):
        if epoch == 0:
            return b'\x00' * 32
        return self.chain.get_block_by_number(epoch*self.epoch_length-1).hash

    def mk_vote_tx(self, vote_msg):
        casper_ct = abi.ContractTranslator(casper_utils.casper_abi)
        vote_func = casper_ct.encode('vote', [vote_msg])
        vote_tx = self.mk_transaction(to=self.chain.casper_address, nonce=0, gasprice=0,
                                      value=0, startgas=1000000, data=vote_func, signed=False)
        return vote_tx

    def mk_logout_tx(self, logout_msg, nonce):
        casper_ct = abi.ContractTranslator(casper_utils.casper_abi)
        logout_func = casper_ct.encode('logout', [logout_msg])
        logout_tx = self.mk_transaction(self.chain.casper_address, data=logout_func)
        return logout_tx

    def mk_withdraw_tx(self, validator_index):
        casper_ct = abi.ContractTranslator(casper_utils.casper_abi)
        withdraw_func = casper_ct.encode('withdraw', [validator_index])
        withdraw_tx = self.mk_transaction(self.chain.casper_address, data=withdraw_func)
        return withdraw_tx
