/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio

import scala.reflect.macros._

private[scio] object MagnoliaMacros {
  import magnolia1._

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def genWithoutAnnotations[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]

    if (wtt <:< typeOf[Iterable[_]]) {
      c.abort(
        c.enclosingPosition,
        s"Automatic coder derivation can't derive a Coder for $wtt <: Seq"
      )
    }

    val magnoliaTree = Magnolia.gen[T](c)

    // format: off
    // Remove annotations from magnolia since they are
    // not serializable and we don't use them anyway
    val removeAnnotations = new Transformer {
      override def transform(tree: Tree): c.universe.Tree = {
        tree match {
          case q"$caseClass($typeName, $isObject, $isValueClass, $parametersArray, $_, $_, $_)" if caseClass.symbol.name == TypeName("CaseClass") =>
            super.transform(
              q"$caseClass($typeName, $isObject, $isValueClass, $parametersArray, Array.empty[Any], Array.empty[Any], Array.empty[Any])"
            )
          case q"Param.apply[$tc_, $t_, $p_]($name, $typeNameParam, $idx, $isRepeated, $typeclassParam, $defaultVal, $_, $_, $_)" =>
            super.transform(
              q"_root_.magnolia1.Param[$tc_, $t_, $p_]($name, $typeNameParam, $idx, $isRepeated, $typeclassParam, $defaultVal, Array.empty[Any], Array.empty[Any], Array.empty[Any])"
            )
          case q"new SealedTrait($typeName, $subtypesArray, $_, $_, $_)" =>
            super.transform(
              q"new _root_.magnolia1.SealedTrait($typeName, $subtypesArray, Array.empty[Any], Array.empty[Any], Array.empty[Any])"
            )
          case q"Subtype[$tc_, $t_, $s_]($name, $idx, $_, $_, $_, $tc, $isType, $asType)" =>
            super.transform(
              q"_root_.magnolia1.Subtype[$tc_, $t_,$s_]($name, $idx, Array.empty[Any], Array.empty[Any], Array.empty[Any], $tc, $isType, $asType)"
            )
          case t =>
            super.transform(t)
        }
      }
    }
    // format: on

    removeAnnotations.transform(magnoliaTree)
  }
}
