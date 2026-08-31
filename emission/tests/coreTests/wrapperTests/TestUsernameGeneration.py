# Standard imports
import unittest
from unittest.mock import patch, Mock

# Our imports
from emission.core.wrapper.user import User
import emission.core.wrapper.user as user_wrapper


class TestUsernameGeneration(unittest.TestCase):
  def setUp(self):
    self._old_cache = user_wrapper._ENGLISH_WORDS_CACHE
    user_wrapper._ENGLISH_WORDS_CACHE = None

  def tearDown(self):
    user_wrapper._ENGLISH_WORDS_CACHE = self._old_cache

  def testGenerateUsernameUsesMockedWordsAndRules(self):
    mocked_words = "anchor\napple\nbanana\nbeacon\ncherry\n"

    class _DeterministicRandom:
      def sample(self, seq, k):
        return ["a", "b"]

      def choice(self, seq):
        if seq == ["anchor", "apple"]:
          return "anchor"
        if seq == ["banana", "beacon"]:
          return "banana"
        return seq[0]

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = mocked_words

    with patch.object(user_wrapper.requests, "get", return_value=mock_response), \
         patch.object(user_wrapper.random, "SystemRandom", return_value=_DeterministicRandom()):
      username = User.generate_username()

    first_word, second_word = username.split("_")
    allowed_words = {"anchor", "apple", "banana", "beacon", "cherry"}

    self.assertIn(first_word, allowed_words)
    self.assertIn(second_word, allowed_words)
    self.assertGreaterEqual(len(first_word), 3)
    self.assertGreaterEqual(len(second_word), 3)
    self.assertNotEqual(first_word[0], second_word[0])

  def testGenerateUsernameExcludesShortAlphaWords(self):
    mocked_words = "a\nan\nbe\nat\nalpha\napple\nbravo\nbeacon\n"

    class _DeterministicRandom:
      def sample(self, seq, k):
        return ["a", "b"]

      def choice(self, seq):
        return seq[0]

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = mocked_words

    with patch.object(user_wrapper.requests, "get", return_value=mock_response), \
         patch.object(user_wrapper.random, "SystemRandom", return_value=_DeterministicRandom()):
      words_by_first_char = user_wrapper._load_words()
      username = User.generate_username()

    cached_words = set()
    for words in words_by_first_char.values():
      cached_words.update(words)

    self.assertNotIn("a", cached_words)
    self.assertNotIn("an", cached_words)
    self.assertNotIn("be", cached_words)
    self.assertNotIn("at", cached_words)

    first_word, second_word = username.split("_")
    self.assertGreaterEqual(len(first_word), 3)
    self.assertGreaterEqual(len(second_word), 3)
    self.assertNotEqual(first_word[0], second_word[0])
    self.assertIn(first_word, {"alpha", "apple"})
    self.assertIn(second_word, {"bravo", "beacon"})

  def testGenerateUsernameRaisesIfOnlyOneStartingCharacter(self):
    mocked_words = "apple\napril\nanchor\n"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = mocked_words

    with patch.object(user_wrapper.requests, "get", return_value=mock_response):
      with self.assertRaisesRegex(ValueError, "at least two starting characters"):
        User.generate_username()
