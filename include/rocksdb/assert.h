/* Copyright (C) 1991-2016 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <http://www.gnu.org/licenses/>.  */

/*
 *	ISO C99 Standard: 7.2 Diagnostics	<assert.h>
 */

#ifdef TERARK_ASSERT_H

#undef TERARK_ASSERT_H
#undef terark_assert
#undef TERARK_ASSERT_VOID_CAST

#endif /* assert.h    */

#define TERARK_ASSERT_H 1
#include <features.h>

#if defined __cplusplus && __GNUC_PREREQ(2, 95)
#define TERARK_ASSERT_VOID_CAST static_cast<void>
#else
#define TERARK_ASSERT_VOID_CAST (void)
#endif

/* void assert (int expression);

   If TERARK_NDEBUG is defined, do nothing.
   If not, and EXPRESSION is zero, print an error message and abort.  */
// #define    TERARK_NDEBUG
#ifdef TERARK_NDEBUG

#define terark_assert(expr) (TERARK_ASSERT_VOID_CAST(0))

/* void assert_perror (int errnum);

   If TERARK_NDEBUG is defined, do nothing.  If not, and ERRNUM is not zero,
   print an error message with the error text for ERRNUM and abort. (This is a
   GNU extension.) */

#else /* Not TERARK_NDEBUG.  */

#ifndef TERARK_ASSERT_H_DECLS
#define TERARK_ASSERT_H_DECLS
__BEGIN_DECLS

/* This prints an "Assertion failed" message and aborts.  */
extern void terark_assert_fail(const char *assertion, const char *file,
                               unsigned int line, const char *function) __THROW
    __attribute__((__noreturn__));

__END_DECLS
#endif /* Not _TERARK_ASSERT_H_DECLS */

#define terark_assert(expr)                               \
  ((expr) ? TERARK_ASSERT_VOID_CAST(0)                    \
          : terark_assert_fail(#expr, __FILE__, __LINE__, \
                               TERARK_ASSERT_FUNCTION))

/* Version 2.4 and later of GCC define a magical variable `__PRETTY_FUNCTION__'
   which contains the name of the function currently being defined.
   This is broken in G++ before version 2.6.
   C9x has a similar variable called __func__, but prefer the GCC one since
   it demangles C++ function names.  */
#if defined __cplusplus ? __GNUC_PREREQ(2, 6) : __GNUC_PREREQ(2, 4)
#define TERARK_ASSERT_FUNCTION __PRETTY_FUNCTION__
#else
#if defined __STDC_VERSION__ && __STDC_VERSION__ >= 199901L
#define TERARK_ASSERT_FUNCTION __func__
#else
#define TERARK_ASSERT_FUNCTION ((const char *)0)
#endif
#endif

#endif /* TERARK_NDEBUG.  */

#if defined __USE_ISOC11 && !defined __cplusplus
#undef static_assert
#define static_assert _Static_assert
#endif
